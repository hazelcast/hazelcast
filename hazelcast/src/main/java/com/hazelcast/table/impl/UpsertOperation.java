package com.hazelcast.table.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.OpCodes;

import java.util.Map;

import static com.hazelcast.internal.nio.Packet.VERSION;

public class UpsertOperation extends Op {

    public UpsertOperation() {
        super(OpCodes.TABLE_UPSERT);
    }

    @Override
    public int run() throws Exception {
        readName();

        TableManager tableManager = managers.tableManager;
        Map map = tableManager.get(partitionId, name);

        //System.out.println("write: begin offset:"+out.position());

        out.writeByte(VERSION);

        out.writeChar(Packet.FLAG_OP_RESPONSE);

        // partitionId
        out.writeInt(33);

        // fake position
        int sizePos = out.position();
        out.writeInt(-1);

        //System.out.println("write: position call id: "+out.position());
        //System.out.println("write: data offset:"+Packet.DATA_OFFSET);
        // callId
        out.writeLong(callId);

        // the length of the packet
        int len = out.position() - sizePos - Bits.INT_SIZE_IN_BYTES;
        //System.out.println("len: " + len);
        out.writeInt(sizePos, len);

        //here we load the key
        //here we load the value
        // and insert it.

        return Op.RUN_CODE_DONE;
    }

/**
 *
 byte version = src.get();
 if (VERSION != version) {
 throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
 + VERSION + ", Incoming -> " + version);
 }

 flags = src.getChar();
 partitionId = src.getInt();
 size = src.getInt();
 */
}
