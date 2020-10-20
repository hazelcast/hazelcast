package com.hazelcast.internal.netty;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.server.tcp.TcpServerConnectionManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class LinkDecoder extends ByteToMessageDecoder {

    private final TcpServerConnectionManager serverConnectionManager;
    private final Address thisAddress;

    public LinkDecoder(Address thisAddress, ServerConnectionManager serverConnectionManager) {
        this.thisAddress = thisAddress;
        this.serverConnectionManager = (TcpServerConnectionManager)serverConnectionManager;
    }

    public String debug(ChannelHandlerContext ctx){
        return thisAddress+"["+ctx.channel().localAddress()+"->"+ctx.channel().remoteAddress()+"] ";
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int length = in.readInt();
        CharSequence charSequence = in.readCharSequence(length, StandardCharsets.UTF_8);
        int port = in.readInt();
        Address remoteAddress = new Address(charSequence.toString(), port);

        int planeIndex = in.readInt();

        TcpServerConnection connection = serverConnectionManager.get(planeIndex, remoteAddress);
        if(connection == null){
            System.out.println("Connection not found for remote address:"+remoteAddress);
        }
        connection.nettyChannel = ctx.channel();
        //System.out.println(debug(ctx)+"found connection:"+connection);

        ctx.attr(TcpServerConnection.CONNECTION).set(connection);

        ctx.pipeline().remove(this);
    }
}