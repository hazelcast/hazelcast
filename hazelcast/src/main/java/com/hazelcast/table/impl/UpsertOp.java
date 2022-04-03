package com.hazelcast.table.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.OpCodes;
import com.hazelcast.table.Item;

import java.util.Map;

import static com.hazelcast.internal.nio.Packet.VERSION;

public class UpsertOp extends Op {

    public UpsertOp() {
        super(OpCodes.TABLE_UPSERT);
    }

    @Override
    public int run() throws Exception {
        readName();

        TableManager tableManager = managers.tableManager;
        Map map = tableManager.get(partitionId, name);

        Item item = new Item();
        item.key = request.readLong();
        item.a = request.readInt();
        item.b = request.readInt();
        map.put(item.key, item);

        //System.out.println("write: begin offset:"+out.position());

        response.writeByte(VERSION);

        response.writeChar((char)Packet.FLAG_OP_RESPONSE);

        // partitionId
        response.writeInt(partitionId);

        // fake position
        int sizePos = response.position();
        response.writeInt(-1);

        //System.out.println("write: position call id: "+out.position());
        //System.out.println("write: data offset:"+Packet.DATA_OFFSET);
        // callId
        response.writeLong(callId);

        // the length of the packet
        int len = response.position() - sizePos - Bits.INT_SIZE_IN_BYTES;
        //System.out.println("len: " + len);
        response.setInt(sizePos, len);

        //here we load the key
        //here we load the value
        // and insert it.

        return Op.RUN_CODE_DONE;
    }
}
