package com.hazelcast.internal.netty;

import com.hazelcast.cluster.Address;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class LinkEncoder extends MessageToByteEncoder {
    private final Address thisAddress;

    public LinkEncoder(Address thisAddress) {
        this.thisAddress = thisAddress;
    }

    public String debug(ChannelHandlerContext ctx) {
        return thisAddress + "[" + ctx.channel().localAddress() + "->" + ctx.channel().remoteAddress() + "] ";
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object a, ByteBuf out) throws Exception {
        if (!(a instanceof Link)) {
            System.out.println("Skipping Link encoder for: " + a);
            return;
        }

        Link link = (Link)a;
        Address address = link.address;
        //System.out.println(debug(ctx) + "AddressEncoder: Send address:" + address);

        out.writeInt(address.getHost().length());
        out.writeCharSequence(address.getHost(), StandardCharsets.UTF_8);
        out.writeInt(address.getPort());
        out.writeInt(link.plane);
        ctx.pipeline().remove(this);
    }

}