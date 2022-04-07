package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.reactor.OpCodes;
import com.hazelcast.spi.impl.reactor.Op;

public class SelectByKeyOperation extends Op {

    public SelectByKeyOperation() {
        super(OpCodes.TABLE_SELECT_BY_KEY);
    }

    @Override
    public int run() {
        return Op.COMPLETED;
    }

    @Override
    public void clear() {
    }
}
