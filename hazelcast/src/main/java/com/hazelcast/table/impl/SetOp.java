package com.hazelcast.table.impl;

import com.hazelcast.tpc.offheapmap.Bin;
import com.hazelcast.tpc.offheapmap.OffheapMap;
import com.hazelcast.tpc.requestservice.Op;
import com.hazelcast.tpc.requestservice.OpCodes;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

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

        response.writeResponseHeader(partitionId, callId())
                .writeComplete();

        return COMPLETED;
    }
}
