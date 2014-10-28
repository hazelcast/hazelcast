package com.hazelcast.map.impl;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;

/**
 * Helper methods for {@link MapServiceContext}
 */
interface MapServiceContextSupport {

    long getNow();

    Object toObject(Object data);

    Data toData(Object object, PartitioningStrategy partitionStrategy);

    Data toData(Object object);

    boolean compare(String mapName, Object value1, Object value2);
}
