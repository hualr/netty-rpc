package com.netty.rpc.server.core;

import com.google.common.base.Preconditions;
import com.netty.rpc.server.registry.ServiceRegistry;
import com.netty.rpc.util.ServiceUtil;
import com.netty.rpc.util.ThreadPoolUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer implements Server {
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private Thread thread;
    private final String serverAddress;
    private final ServiceRegistry serviceRegistry;
    private final Map<String, Object> serviceMap = new HashMap<>();

    private Supplier<Runnable> runnableSupplier;

    public NettyServer(String serverAddress, String registryAddress) {
        Preconditions.checkNotNull(serverAddress, "serviceAddress is null");
        Preconditions.checkNotNull(registryAddress, "registryAddress is null");
        this.serverAddress = serverAddress;
        this.serviceRegistry = new ServiceRegistry(registryAddress);
        init();
    }

    public void init(){
        if (runnableSupplier != null) {
            return;
        }
        Runnable runnable= () -> {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new RpcServerInitializer(serviceMap, ThreadPoolUtil.makeServerThreadPool(
                                NettyServer.class.getSimpleName(), 16, 32)))
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);

                String host = serverAddress.split(":")[0];
                int port = Integer.parseInt(serverAddress.split(":")[1]);
                ChannelFuture future = bootstrap.bind(host, port).sync();
                if (serviceRegistry != null) {
                    serviceRegistry.registerService(host, port, serviceMap);
                }
                logger.info("Server started on port {}", port);
                future.channel().closeFuture().sync();
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    logger.info("Rpc server remoting server stop");
                } else {
                    logger.error("Rpc server remoting server error", e);
                }
            } finally {
                try {
                    serviceRegistry.unregisterService();
                    workerGroup.shutdownGracefully();
                    bossGroup.shutdownGracefully();
                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        };
        runnableSupplier=()->runnable;
    }

    public void addService(String interfaceName, String version, Object serviceBean) {
        logger.info("Adding service, interface: {}, version: {}, bean：{}", interfaceName, version, serviceBean);
        String serviceKey = ServiceUtil.makeServiceKey(interfaceName, version);
        serviceMap.put(serviceKey, serviceBean);
    }

    @Override
    public void start() {
        thread = new Thread(runnableSupplier.get());
        thread.start();
    }

    @Override
    public void stop() {
        // destroy server thread
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }
    }

}
