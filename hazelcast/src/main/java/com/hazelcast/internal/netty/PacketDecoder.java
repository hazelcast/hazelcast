package com.hazelcast.internal.netty;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;

import java.util.List;

import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Packet.VERSION;

public class PacketDecoder extends ByteToMessageDecoder {
    static final int HEADER_SIZE = BYTE_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    private final Address thisAddress;

    private int packetsDecoded = 0;

    private int valueOffset;
    private int size;
    private boolean headerComplete;
    private char flags;
    private int partitionId;
    private byte[] payload;

    public PacketDecoder(Address thisAddress) {
        this.thisAddress = thisAddress;
    }

    public String debug(ChannelHandlerContext ctx) {
        return thisAddress + "[" + ctx.channel().localAddress() + "->" + ctx.channel().remoteAddress() + "] ";
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf src, List<Object> out) {
        try {
            if (!headerComplete) {
                if (src.readableBytes() < HEADER_SIZE) {
                    return;
                }

                byte version = src.readByte();
                if (VERSION != version) {
                    throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                            + VERSION + ", Incoming -> " + version);
                }

                flags = src.readChar();
                partitionId = src.readInt();
                size = src.readInt();
                headerComplete = true;
            }

            if (readValue(src)) {
                Packet packet = new Packet(payload, partitionId).resetFlagsTo(flags);
                Attribute attr = ctx.channel().attr(TcpServerConnection.CONNECTION);
                TcpServerConnection conn = (TcpServerConnection) attr.get();
                if (conn == null) {
                    throw new RuntimeException("Failed to find connection for: " + debug(ctx));
                }
                packet.setConn(conn);
                reset();
                //System.out.println(debug(ctx) + ", index:" + packetsDecoded + " Received packet: " + packet);
                out.add(packet);
                packetsDecoded++;
            }
        } catch (Exception e) {
            System.out.println("Failed at packet " + packetsDecoded);
            throw e;
        }

        //System.out.println(debug(ctx)+" decode");
    }

    private void reset() {
        headerComplete = false;
        payload = null;
        valueOffset = 0;
    }

    private boolean readValue(ByteBuf src) {
        if (payload == null) {
            payload = new byte[size];
        }

        if (size > 0) {
            int bytesReadable = src.readableBytes();

            int bytesNeeded = size - valueOffset;

            boolean done;
            int bytesRead;
            if (bytesReadable >= bytesNeeded) {
                bytesRead = bytesNeeded;
                done = true;
            } else {
                bytesRead = bytesReadable;
                done = false;
            }

            // read the data from the byte-buffer into the bytes-array.
            src.readBytes(payload, valueOffset, bytesRead);
            valueOffset += bytesRead;

            if (!done) {
                return false;
            }
        }

        return true;
    }
}
