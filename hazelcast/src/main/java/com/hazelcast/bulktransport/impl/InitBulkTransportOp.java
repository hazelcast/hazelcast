package com.hazelcast.bulktransport.impl;

import com.hazelcast.tpc.offheapmap.OffheapMap;
import com.hazelcast.tpc.requestservice.Op;
import com.hazelcast.tpc.requestservice.OpCodes;
import com.hazelcast.table.impl.TableManager;

import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class InitBulkTransportOp extends Op {

    public InitBulkTransportOp() {
        super(OpCodes.INIT_BULK_TRANSPORT);
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
