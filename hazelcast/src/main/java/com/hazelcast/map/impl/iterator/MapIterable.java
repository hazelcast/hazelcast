package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapIterable<K, V> implements Iterable<Map.Entry<K, V>> {
    private final MapProxyImpl<K, V> mapProxy;
    private final int fetchSize;
    private final IPartition[] partitions;
    private final boolean prefetchValues;

    public MapIterable(MapProxyImpl<K, V> mapProxy,
                       int fetchSize, IPartition[] partitions,
                       boolean prefetchValues
                       ) {
        this.mapProxy = mapProxy;
        this.partitions = partitions;
        this.fetchSize = fetchSize;
        this.prefetchValues = prefetchValues;
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        // create the partition iterators
        List<Iterator<Map.Entry<K, V>>> partitionIterators = Arrays.stream(partitions)
                .map(partition -> mapProxy.iterator(fetchSize, partition.getPartitionId(), prefetchValues))
                .collect(Collectors.toList());
        return new MapIterator<>(partitionIterators);
    }
}