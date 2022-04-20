package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;

import java.util.Map;

import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQ_CALL_ID;

public class SetOp extends Op {

    public SetOp() {
        super(OpCodes.SET);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        Map map = tableManager.get(partitionId, null);

        int keyLen = request.readInt();
        byte[] key = new byte[keyLen];
        request.readBytes(key, keyLen);

        int valueLen = request.readInt();
        byte[] value = new byte[valueLen];
        request.readBytes(value, valueLen);

        //map.put(item.key, item);

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQ_CALL_ID))
                .writeComplete();

        return COMPLETED;
    }
}
