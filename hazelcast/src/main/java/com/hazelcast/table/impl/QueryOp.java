package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.offheapmap.ExampleQuery;
import com.hazelcast.spi.impl.offheapmap.OffheapMap;
import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

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

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeLong(query.result)
                .writeComplete();

        return COMPLETED;
    }
}
