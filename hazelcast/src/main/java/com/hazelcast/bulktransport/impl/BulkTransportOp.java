package com.hazelcast.bulktransport.impl;

import com.hazelcast.spi.impl.offheapmap.OffheapMap;
import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;
import com.hazelcast.table.impl.TableManager;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class BulkTransportOp extends Op {

    public BulkTransportOp() {
        super(OpCodes.BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID));
        response.writeComplete();

        return COMPLETED;
    }
}
