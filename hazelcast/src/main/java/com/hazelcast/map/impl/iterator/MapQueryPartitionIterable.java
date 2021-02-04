package com.hazelcast.map.impl.iterator;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;

public class MapQueryPartitionIterable<K, V, R> implements Iterable<R> {
    private final MapProxyImpl<K, V> mapProxy;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, R> projection;
    private final int fetchSize;
    private final int partitionId;

    public MapQueryPartitionIterable(
            MapProxyImpl<K, V> mapProxy,
            int fetchSize, int partitionId,
            Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate
    ) {
        this.mapProxy = mapProxy;
        this.partitionId = partitionId;
        this.fetchSize = fetchSize;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Nonnull
    @Override
    public Iterator<R> iterator() {
        return mapProxy.iterator(fetchSize, partitionId, projection, predicate);
    }
}
