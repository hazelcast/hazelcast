package com.hazelcast.internal.netty;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class AddressDecoder extends ByteToMessageDecoder {

    private final ServerConnectionManager serverConnectionManager;
    private final Address thisAddress;

    public AddressDecoder(Address thisAddress, ServerConnectionManager serverConnectionManager) {
        this.thisAddress = thisAddress;
        this.serverConnectionManager = serverConnectionManager;
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
        //System.out.println(debug(ctx)+"Read remote address:"+remoteAddress);

//        for(ServerConnection con: serverConnectionManager.getConnections()){
//            System.out.println("      "+con);
//        }

        TcpServerConnection connection = (TcpServerConnection)serverConnectionManager.get(remoteAddress);
        if(connection == null){
            System.out.println("Connection not found for remote address:"+remoteAddress);
        }
        connection.nettyChannel = ctx.channel();
        //System.out.println(debug(ctx)+"found connection:"+connection);

        ctx.attr(TcpServerConnection.CONNECTION).set(connection);

        ctx.pipeline().remove(this);
    }
}