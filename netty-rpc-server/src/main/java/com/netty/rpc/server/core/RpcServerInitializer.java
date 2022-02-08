package com.netty.rpc.server.core;

import com.netty.rpc.codec.Beat;
import com.netty.rpc.codec.RpcDecoder;
import com.netty.rpc.codec.RpcEncoder;
import com.netty.rpc.codec.RpcRequest;
import com.netty.rpc.codec.RpcResponse;
import com.netty.rpc.serializer.Serializer;
import com.netty.rpc.serializer.kryo.KryoSerializer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RpcServerInitializer extends ChannelInitializer<SocketChannel> {
    private final Map<String, Object> handlerMap;
    private final ThreadPoolExecutor threadPoolExecutor;

    public RpcServerInitializer(Map<String, Object> handlerMap, ThreadPoolExecutor threadPoolExecutor) {
        this.handlerMap = handlerMap;
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    public void initChannel(SocketChannel channel) throws Exception {
//        Serializer serializer = ProtostuffSerializer.class.newInstance();
//        Serializer serializer = HessianSerializer.class.newInstance();
        Serializer serializer = KryoSerializer.class.newInstance();
        channel.pipeline()
            .addLast(new IdleStateHandler(0, 0, Beat.BEAT_TIMEOUT, TimeUnit.SECONDS))
            .addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0))
            .addLast(new RpcDecoder(RpcRequest.class, serializer))
            .addLast(new RpcEncoder(RpcResponse.class, serializer))
            .addLast(new RpcServerHandler(handlerMap, threadPoolExecutor));
    }
}
