package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.reactor.OpCodes;
import com.hazelcast.spi.impl.reactor.Op;

import java.io.Serializable;
import java.util.Map;

public class UpsertOperation extends Op implements Serializable {

    public UpsertOperation() {
        super(OpCodes.TABLE_UPSERT);
    }

    @Override
    public int run()throws Exception {
        readName();

        TableManager tableManager = managers.tableManager;
        Map map = tableManager.get(partitionId, name);


        //here we load the key
        //here we load the value
        // and insert it.

        return Op.RUN_CODE_DONE;
    }


}
