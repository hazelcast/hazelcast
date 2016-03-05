package com.hazelcast.jet.memory.impl.aggregator;

import com.hazelcast.nio.Bits;
import com.hazelcast.jet.memory.spi.operations.functors.IntegerFunctor;

public class SumFunctor extends IntegerFunctor {
    @Override
    public int operate(int oldValue, int newValue) {
        return oldValue + newValue;
    }

    @Override
    public boolean isAssociative() {
        return true;
    }

    @Override
    public long getDataAddress(long address) {
        //ElementsCount , Type of value
        return address + Bits.INT_SIZE_IN_BYTES + Bits.BYTE_SIZE_IN_BYTES;
    }
}
