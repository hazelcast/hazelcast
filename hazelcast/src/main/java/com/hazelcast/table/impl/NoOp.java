package com.hazelcast.table.impl;


import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public final class NoOp extends Op {

    public final static AtomicLong counter = new AtomicLong();

    public NoOp() {
        super(OpCodes.NOOP);
    }

    @Override
    public int run() throws Exception {
        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeComplete();

        return COMPLETED;
    }
}
