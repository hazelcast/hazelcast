package com.hazelcast.map.impl.iterator;

import com.hazelcast.map.impl.proxy.MapProxyImpl;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;

public class MapPartitionIterable<K, V> implements Iterable<Map.Entry<K, V>> {
    private final MapProxyImpl<K, V> mapProxy;
    private final int fetchSize;
    private final int partitionId;
    private final boolean prefetchValues;

    public MapPartitionIterable(MapProxyImpl<K, V> mapProxy, int fetchSize, int partitionId, boolean prefetchValues) {
        this.mapProxy = mapProxy;
        this.partitionId = partitionId;
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
    }

    @Nonnull
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return mapProxy.iterator(fetchSize, partitionId, prefetchValues);
    }
}
