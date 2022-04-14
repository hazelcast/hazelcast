package com.hazelcast.table.impl;


import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_REQUEST_CALL_ID;

public class NoOp extends Op {

    public final static AtomicLong counter = new AtomicLong();

    public NoOp() {
        super(OpCodes.TABLE_NOOP);
    }

    @Override
    public int run() throws Exception {
        //System.out.println("NoOp.run");

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQUEST_CALL_ID))
                .completeWriting();

        return Op.COMPLETED;
    }
}
