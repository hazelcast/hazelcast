package com.hazelcast.table.impl;



import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.OpCodes;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.impl.reactor.Frame.OFFSET_REQUEST_CALL_ID;

public class NoOp extends Op {

    public final static AtomicLong counter = new AtomicLong();

    public NoOp() {
        super(OpCodes.TABLE_NOOP);
    }

    @Override
    public int run() throws Exception {
         response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQUEST_CALL_ID))
                .completeWriting();

        return Op.RUN_CODE_DONE;
    }
}
