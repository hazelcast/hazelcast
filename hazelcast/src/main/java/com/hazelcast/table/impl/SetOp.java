package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.offheapmap.Bin;
import com.hazelcast.spi.impl.offheapmap.OffheapMap;
import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public final class SetOp extends Op {

    private final Bin key = new Bin();
    private final Bin value = new Bin();

    public SetOp() {
        super(OpCodes.SET);
    }

    @Override
    public void clear() {
        key.clear();
        value.clear();
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        key.init(request);
        value.init(request);

        map.set(key, value);

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeComplete();

        return COMPLETED;
    }
}
