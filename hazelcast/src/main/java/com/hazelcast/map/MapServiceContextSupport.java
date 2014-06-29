package com.hazelcast.map;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

interface MapServiceContextSupport {

    long getNow();

    long convertTime(long seconds, TimeUnit unit);

    Object toObject(Object data);

    Data toData(Object object, PartitioningStrategy partitionStrategy);

    Data toData(Object object);

    boolean compare(String mapName, Object value1, Object value2);
}
