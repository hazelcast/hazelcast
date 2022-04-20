package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class GetOp extends Op {

    public GetOp() {
        super(OpCodes.GET);
    }

    @Override
    public int run() throws Exception {
        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeComplete();

        return COMPLETED;
    }
}