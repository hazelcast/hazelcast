package com.hazelcast.table.impl;

import com.hazelcast.tpc.offheapmap.Bin;
import com.hazelcast.tpc.offheapmap.Bout;
import com.hazelcast.tpc.offheapmap.OffheapMap;
import com.hazelcast.tpc.requestservice.Op;
import com.hazelcast.tpc.requestservice.OpCodes;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public final class GetOp extends Op {

    private final Bin key = new Bin();
    private final Bout value = new Bout();

    public GetOp() {
        super(OpCodes.GET);
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

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID));
        value.init(response);
        map.get(key, value);
        response.writeComplete();

        return COMPLETED;
    }
}