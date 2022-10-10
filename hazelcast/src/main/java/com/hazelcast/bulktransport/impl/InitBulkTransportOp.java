package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.alto.offheapmap.OffheapMap;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.alto.OpCodes;
import com.hazelcast.table.impl.TableManager;

import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;

public class InitBulkTransportOp extends Op {

    public InitBulkTransportOp() {
        super(OpCodes.INIT_BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        return COMPLETED;
    }
}
