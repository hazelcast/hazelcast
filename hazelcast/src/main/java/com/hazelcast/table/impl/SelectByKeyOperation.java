package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.requestservice.OpCodes;
import com.hazelcast.spi.impl.requestservice.Op;

import static com.hazelcast.spi.impl.requestservice.Op.COMPLETED;
import static com.hazelcast.spi.impl.requestservice.OpCodes.TABLE_SELECT_BY_KEY;

public class SelectByKeyOperation extends Op {

    public SelectByKeyOperation() {
        super(TABLE_SELECT_BY_KEY);
    }

    @Override
    public int run() {
        return COMPLETED;
    }

    @Override
    public void clear() {
    }
}
