package com.netty.rpc.client.connect;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.handler.RpcClientInitializer;
import com.netty.rpc.client.route.api.RpcLoadBalance;
import com.netty.rpc.client.route.impl.RpcLoadBalanceRoundRobin;
import com.netty.rpc.protocol.RpcServiceInfo;
import com.netty.rpc.protocol.ServerConfigInfo;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC Connection Manager
 * Created by luxiaoxun on 2016-03-16.
 */
public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(4);
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 8,
            600L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000));

    private final Map<ServerConfigInfo, RpcClientHandler> connectedServerNodes = new ConcurrentHashMap<>();
    private final Set<ServerConfigInfo> serverConfigInfoSet = new CopyOnWriteArraySet<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final Condition connected = lock.newCondition();
    private long waitTimeout = 5000;
    private final RpcLoadBalance loadBalance = new RpcLoadBalanceRoundRobin();
    private volatile boolean isRunning = true;

    private ConnectionManager() {
    }

    private static class SingletonHolder {
        private static final ConnectionManager INSTANCE = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public void updateConnectedServer(List<ServerConfigInfo> serviceList) {
        // Now using 2 collections to manage the service info and TCP connections because making the connection is async
        // Once service info is updated on ZK, will trigger this function
        // Actually client should only care about the service it is using

        if (serviceList == null || serviceList.isEmpty()) {
            // No available service
            logger.error("No available service!");
            serverConfigInfoSet.forEach(this::removeAndCloseHandler);
        }
        // Update local server nodes cache
        Set<ServerConfigInfo> serviceSet = new HashSet<>(serviceList);
        // Add new server info
        for (final ServerConfigInfo serverConfigInfo : serviceSet) {
            if (!serverConfigInfoSet.contains(serverConfigInfo)) {
                connectServerNode(serverConfigInfo);
            }
        }

        // Close and remove invalid server nodes
        for (ServerConfigInfo serverConfigInfo : serverConfigInfoSet) {
            if (!serviceSet.contains(serverConfigInfo)) {
                logger.info("Remove invalid service::[{}] ", serverConfigInfo.toJson());
                removeAndCloseHandler(serverConfigInfo);
            }
        }
    }


    public void updateConnectedServer(ServerConfigInfo serverConfigInfo, PathChildrenCacheEvent.Type type) {
        if (serverConfigInfo == null) {
            return;
        }
        if (type == PathChildrenCacheEvent.Type.CHILD_ADDED && !serverConfigInfoSet.contains(serverConfigInfo)) {
            connectServerNode(serverConfigInfo);
        } else if (type == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
            //TODO We may don't need to reconnect remote server if the server'IP and server'port are not changed
            removeAndCloseHandler(serverConfigInfo);
            connectServerNode(serverConfigInfo);
        } else if (type == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
            removeAndCloseHandler(serverConfigInfo);
        } else {
            throw new IllegalArgumentException("Unknow type:" + type);
        }
    }

    private void connectServerNode(ServerConfigInfo serverConfigInfo) {
        if (serverConfigInfo.getServiceInfoList() == null || serverConfigInfo.getServiceInfoList().isEmpty()) {
            logger.info("No service on node, host: {}, port: {}", serverConfigInfo.getHost(),
                    serverConfigInfo.getPort());
            return;
        }
        serverConfigInfoSet.add(serverConfigInfo);
        logger.info("New service node, host: {}, port: {}", serverConfigInfo.getHost(), serverConfigInfo.getPort());
        for (RpcServiceInfo serviceProtocol : serverConfigInfo.getServiceInfoList()) {
            logger.info("New service info, name: {}, version: {}", serviceProtocol.getServiceName(),
                    serviceProtocol.getVersion());
        }
        final InetSocketAddress remotePeer = new InetSocketAddress(serverConfigInfo.getHost(),
                serverConfigInfo.getPort());
        threadPoolExecutor.submit(() -> {
            Bootstrap b = new Bootstrap();
            b.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new RpcClientInitializer());

            ChannelFuture channelFuture = b.connect(remotePeer);
            channelFuture.addListener(
                (ChannelFutureListener) channelFuture1 -> {
                    if (!channelFuture1.isSuccess()) {
                        logger.error("Can not connect to remote server, remote peer = [{}]", remotePeer);
                        return;

                    }
                    logger.info("Successfully connect to remote server, remote peer = [{}]", remotePeer);
                    RpcClientHandler handler = channelFuture1.channel().pipeline().get(RpcClientHandler.class);
                    connectedServerNodes.put(serverConfigInfo, handler);
                    handler.setRpcProtocol(serverConfigInfo);
                    signalAvailableHandler();
                });
        });
    }

    private void signalAvailableHandler() {
        lock.lock();
        try {
            connected.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean waitingForHandler() throws InterruptedException {
        lock.lock();
        try {
            logger.warn("Waiting for available service");
            return connected.await(this.waitTimeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    public RpcClientHandler chooseHandler(String serviceKey) throws Exception {
        int size = connectedServerNodes.values().size();
        while (isRunning && size <= 0) {
            try {
                waitingForHandler();
                size = connectedServerNodes.values().size();
            } catch (InterruptedException e) {
                logger.error("Waiting for available service is interrupted!", e);
            }
        }
        ServerConfigInfo serverConfigInfo = loadBalance.route(serviceKey, connectedServerNodes);
        RpcClientHandler handler = connectedServerNodes.get(serverConfigInfo);
        if (handler != null) {
            return handler;
        } else {
            throw new Exception("Can not get available connection");
        }
    }

    private void removeAndCloseHandler(ServerConfigInfo serverConfigInfo) {
        RpcClientHandler handler = connectedServerNodes.get(serverConfigInfo);
        if (handler != null) {
            handler.close();
        }
        connectedServerNodes.remove(serverConfigInfo);
        serverConfigInfoSet.remove(serverConfigInfo);
    }

    public void removeHandler(ServerConfigInfo serverConfigInfo) {
        serverConfigInfoSet.remove(serverConfigInfo);
        connectedServerNodes.remove(serverConfigInfo);
        logger.info("Remove one connection, host: {}, port: {}", serverConfigInfo.getHost(),
                serverConfigInfo.getPort());
    }

    public void stop() {
        isRunning = false;
        for (ServerConfigInfo serverConfigInfo : serverConfigInfoSet) {
            removeAndCloseHandler(serverConfigInfo);
        }
        signalAvailableHandler();
        threadPoolExecutor.shutdown();
        eventLoopGroup.shutdownGracefully();
    }
}
