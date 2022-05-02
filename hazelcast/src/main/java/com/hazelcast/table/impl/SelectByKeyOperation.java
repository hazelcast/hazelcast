package com.hazelcast.table.impl;

import com.hazelcast.tpc.requestservice.Op;

import static com.hazelcast.tpc.requestservice.OpCodes.TABLE_SELECT_BY_KEY;

public final class SelectByKeyOperation extends Op {

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
