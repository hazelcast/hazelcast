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
        out.writeByte(VERSION);

        out.writeChar(Packet.FLAG_OP_RESPONSE);

        // partitionId
        out.writeInt(partitionId);

        // fake position
        int sizePos = out.position();
        out.writeInt(-1);

        out.writeLong(callId);
        int len = out.position() - sizePos - Bits.INT_SIZE_IN_BYTES;
        out.writeInt(sizePos, len);
        return Op.RUN_CODE_DONE;
    }
}
