package com.netty.rpc.client.handler;

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
import java.util.concurrent.TimeUnit;

/**
 * Created by luxiaoxun on 2016-03-16.
 */
public class RpcClientInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
//        Serializer serializer = ProtostuffSerializer.class.newInstance();
//        Serializer serializer = HessianSerializer.class.newInstance();
        Serializer serializer = KryoSerializer.class.newInstance();
        socketChannel.pipeline()
                .addLast(new IdleStateHandler(0, 0, Beat.BEAT_INTERVAL, TimeUnit.SECONDS))
                .addLast(new RpcEncoder(RpcRequest.class, serializer))
                .addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0))
                .addLast(new RpcDecoder(RpcResponse.class, serializer))
                .addLast(new RpcClientHandler());
    }
}
