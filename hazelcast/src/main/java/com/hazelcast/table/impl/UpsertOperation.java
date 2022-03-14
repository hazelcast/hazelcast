package com.hazelcast.table.impl;

import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.OpCodes;

import java.io.IOException;
import java.util.Map;

public class UpsertOperation extends Op {

    public UpsertOperation() {
        super(OpCodes.TABLE_UPSERT);
    }

//    public void serialize(ByteArrayObjectDataOutput out) throws IOException {
//        out.writeByte(OpCodes.TABLE_UPSERT);
//        int length = name.length();
//        out.writeInt(length);
//        for (int k = 0; k < length; k++) {
//            out.writeChar(name.charAt(k));
//        }
//    }

    @Override
    public int run() throws Exception {
        readName();

        TableManager tableManager = managers.tableManager;
        Map map = tableManager.get(partitionId, name);


        //here we load the key
        //here we load the value
        // and insert it.

        return Op.RUN_CODE_DONE;
    }


}
