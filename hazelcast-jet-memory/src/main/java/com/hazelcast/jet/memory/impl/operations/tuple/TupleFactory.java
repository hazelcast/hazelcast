package com.hazelcast.jet.memory.impl.operations.tuple;

import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.DefaultTuple;
import com.hazelcast.jet.memory.spi.operations.ContainerFactory;

public class TupleFactory<K, V> implements ContainerFactory<Tuple<K, V>> {
    @Override
    public Tuple<K, V> createContainer() {
        return new DefaultTuple<K, V>();
    }
}
