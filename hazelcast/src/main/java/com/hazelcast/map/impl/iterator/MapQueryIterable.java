package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapQueryIterable<K, V, R> implements Iterable<R> {
    private final MapProxyImpl<K, V> mapProxy;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, R> projection;
    private final int fetchSizePerPartition;
    private final IPartition[] partitions;

    public MapQueryIterable(MapProxyImpl<K, V> mapProxy,
                            int fetchSize, IPartition[] partitions,
                            Projection<? super Map.Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        this.mapProxy = mapProxy;
        this.partitions = partitions;
        this.fetchSizePerPartition = fetchSize;
        this.predicate = predicate;
        this.projection = projection;
    }

    @NotNull
    @Override
    public Iterator<R> iterator() {
        // create the partition iterators
        List<Iterator<R>> partitionIterators = Arrays.stream(partitions)
                .map(partition -> mapProxy.iterator(fetchSizePerPartition, partition.getPartitionId(), projection, predicate))
                .collect(Collectors.toList());
        return new MapQueryIterator<>(partitionIterators);
    }
}