package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.requestservice.Op;
import com.hazelcast.spi.impl.requestservice.OpCodes;
import com.hazelcast.table.Item;

import java.util.Map;

import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_REQUEST_CALL_ID;

public class UpsertOp extends Op {

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

        response.writeResponseHeader(partitionId, request.getLong(OFFSET_REQUEST_CALL_ID))
                .completeWriting();


        return Op.COMPLETED;
    }
}
