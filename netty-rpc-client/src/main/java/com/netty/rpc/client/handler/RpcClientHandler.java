package com.netty.rpc.client.handler;

import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.codec.Beat;
import com.netty.rpc.codec.RpcRequest;
import com.netty.rpc.codec.RpcResponse;
import com.netty.rpc.protocol.ServerConfigInfo;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by luxiaoxun on 2016-03-14.
 */
public class RpcClientHandler extends SimpleChannelInboundHandler<RpcResponse> {
    private static final Logger logger = LoggerFactory.getLogger(RpcClientHandler.class);

    private final ConcurrentHashMap<String, RpcFuture> pendingRPC = new ConcurrentHashMap<>();
    private volatile Channel channel;
    private SocketAddress remotePeer;
    private ServerConfigInfo serverConfigInfo;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remotePeer = this.channel.remoteAddress();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        this.channel = ctx.channel();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
        String requestId = response.getRequestId();
        logger.debug("Receive response:[{}] ", requestId);
        RpcFuture rpcFuture = pendingRPC.get(requestId);
        if (rpcFuture != null) {
            pendingRPC.remove(requestId);
            rpcFuture.done(response);
        } else {
            logger.warn("Can not get pending response for request id:[{}] ", requestId);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Client caught exception: ", cause);
        ctx.close();
    }

    public void close() {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    public RpcFuture sendRequest(RpcRequest request) {
        RpcFuture rpcFuture = new RpcFuture(request);
        pendingRPC.put(request.getRequestId(), rpcFuture);
        try {
            ChannelFuture channelFuture = channel.writeAndFlush(request).sync();
            if (!channelFuture.isSuccess()) {
                logger.error("Send request {} error", request.getRequestId());
            }
        } catch (InterruptedException e) {
            logger.error("Send request exception: " , e);
        }

        return rpcFuture;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            //Send ping
            sendRequest(Beat.BEAT_PING);
            logger.debug("Client send beat-ping to :[{}]" , remotePeer);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    public void setRpcProtocol(ServerConfigInfo serverConfigInfo) {
        this.serverConfigInfo = serverConfigInfo;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        ConnectionManager.getInstance().removeHandler(serverConfigInfo);
    }
}
