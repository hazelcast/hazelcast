package com.hazelcast.map;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

/**
 * Helper methods for {@link com.hazelcast.map.MapServiceContext}
 */
interface MapServiceContextSupport {

    long getNow();

    long convertTime(long seconds, TimeUnit unit);

    Object toObject(Object data);

    Data toData(Object object, PartitioningStrategy partitionStrategy);

    Data toData(Object object);

    boolean compare(String mapName, Object value1, Object value2);

    /**
     * Returns expiration time.
     * <p/>
     * - Detects potential long overflow and returns {@link java.lang.Long#MAX_VALUE} in an overflow situation.
     * <p/>
     * - If ttl is zero to state an eternal live, it returns {@link java.lang.Long#MAX_VALUE}
     *
     * @param ttl time to live
     * @param now now
     * @return expiration time
     * @throws java.lang.IllegalArgumentException when ttl < 0 or now < 0.
     */
    long getExpirationTime(long ttl, long now);
}
