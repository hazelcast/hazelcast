package com.hazelcast.table.impl;

import com.hazelcast.tpc.offheapmap.ExampleQuery;
import com.hazelcast.tpc.offheapmap.OffheapMap;
import com.hazelcast.tpc.requestservice.Op;
import com.hazelcast.tpc.requestservice.OpCodes;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class QueryOp extends Op {

    // Currently, we always execute the same bogus query.
    // Probably the queryOp should have some query id for prepared queries
    // And we do a lookup based on that query id.
    // This query instance should also be pooled.
    private ExampleQuery query = new ExampleQuery();

    public QueryOp() {
        super(OpCodes.QUERY);
    }

    @Override
    public void clear() {
        query.clear();
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        map.execute(query);

        response.writeResponseHeader(partitionId, callId())
                .writeLong(query.result)
                .writeComplete();

        return COMPLETED;
    }
}
