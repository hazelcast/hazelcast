package com.hazelcast.internal.netty;

import com.hazelcast.internal.nio.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.hazelcast.internal.nio.Packet.VERSION;

public class PacketEncoder extends MessageToByteEncoder<Packet> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf dst) throws Exception {
        // System.out.println("PacketEncoder encode:"+packet);
        dst.writeByte(VERSION);
        dst.writeChar(packet.getFlags());
        dst.writeInt(packet.getPartitionId());
        int size = packet.totalSize();
        dst.writeInt(size);
        dst.writeBytes(packet.toByteArray());
    }
}