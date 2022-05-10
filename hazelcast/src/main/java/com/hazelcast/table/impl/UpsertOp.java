package com.hazelcast.table.impl;

import com.hazelcast.tpc.requestservice.Op;
import com.hazelcast.tpc.requestservice.OpCodes;
import com.hazelcast.table.Item;

import java.util.Map;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public final class UpsertOp extends Op {

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

        response.writeResponseHeader(partitionId, callId())
                .writeComplete();

        return Op.COMPLETED;
    }
}
