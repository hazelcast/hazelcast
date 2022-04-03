package com.hazelcast.table.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.OpCodes;

import static com.hazelcast.internal.nio.Packet.VERSION;

public class NoOp extends Op {

    public NoOp() {
        super(OpCodes.TABLE_NOOP);
    }

    @Override
    public int run() throws Exception {
        response.writeByte(VERSION);

        response.writeChar((char)Packet.FLAG_OP_RESPONSE);

        // partitionId
        response.writeInt(partitionId);

        // fake position
        int sizePos = response.position();
        response.writeInt(-1);

        response.writeLong(callId);
        int len = response.position() - sizePos - Bits.INT_SIZE_IN_BYTES;
        response.setInt(sizePos, len);
        return Op.RUN_CODE_DONE;
    }
}
