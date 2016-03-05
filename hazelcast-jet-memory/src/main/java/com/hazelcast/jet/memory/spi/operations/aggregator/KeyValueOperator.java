package com.hazelcast.jet.memory.spi.operations.aggregator;

import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.Spillable;
import com.hazelcast.jet.memory.spi.operations.State;
import com.hazelcast.nio.Disposable;

import java.util.Iterator;

public interface KeyValueOperator<T> extends Spillable, Disposable {
    void setComparator(BinaryComparator comparator);

    Iterator<T> iterator();
}
