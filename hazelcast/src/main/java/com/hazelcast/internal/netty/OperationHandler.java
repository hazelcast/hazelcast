package com.hazelcast.internal.netty;

import com.hazelcast.internal.nio.Packet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Consumer;

public class OperationHandler extends SimpleChannelInboundHandler<Packet> {

    private final Consumer<Packet> dispatcher;

    public OperationHandler(Consumer<Packet> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        //System.out.println("Operation handler:"+packet);
        try {
            dispatcher.accept(packet);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
