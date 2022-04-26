package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.offheapmap.ExampleQuery;
import com.hazelcast.spi.impl.offheapmap.OffheapMap;
import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class QueryOp extends Op {

    private ExampleQuery query = new ExampleQuery();

    public QueryOp() {
        super(OpCodes.QUERY);
    }

    @Override
    public void clear() {

    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        map.execute(query);

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeLong(query.bogus)
                .writeComplete();

        return COMPLETED;
    }
}
