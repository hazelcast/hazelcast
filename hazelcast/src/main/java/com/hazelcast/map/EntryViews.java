package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordStatistics;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class EntryViews {

    private EntryViews() {
    }

    /**
     * Creates a null entry view that has only key and no value.
     *
     * @param key the key object which will be wrapped in {@link com.hazelcast.core.EntryView}.
     * @param <K> the type of key.
     * @param <V> the type of value.
     * @return returns  null entry view.
     */
    public static <K, V> EntryView<K, V> createNullEntryView(K key) {
        return new NullEntryView<K, V>(key);
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView(K key, V value, Record record) {
        final SimpleEntryView simpleEntryView = new SimpleEntryView(key, value);
        simpleEntryView.setCost(record.getCost());
        simpleEntryView.setVersion(record.getVersion());
        simpleEntryView.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());
        simpleEntryView.setLastAccessTime(record.getLastAccessTime());
        simpleEntryView.setLastUpdateTime(record.getLastUpdateTime());
        simpleEntryView.setTtl(record.getTtl());
        simpleEntryView.setCreationTime(record.getCreationTime());

        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            simpleEntryView.setHits(statistics.getHits());
            simpleEntryView.setExpirationTime(statistics.getExpirationTime());
            simpleEntryView.setLastStoredTime(statistics.getLastStoredTime());
        }
        return simpleEntryView;
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView() {
        return new SimpleEntryView<K, V>();
    }

    public static <K, V> EntryView<K, V> createLazyEntryView(K key, V value, Record record
            , SerializationService serializationService, MapMergePolicy mergePolicy) {
        final LazyEntryView lazyEntryView = new LazyEntryView(key, value, serializationService, mergePolicy);
        lazyEntryView.setCost(record.getCost());
        lazyEntryView.setVersion(record.getVersion());
        lazyEntryView.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());
        lazyEntryView.setLastAccessTime(record.getLastAccessTime());
        lazyEntryView.setLastUpdateTime(record.getLastUpdateTime());
        lazyEntryView.setTtl(record.getTtl());
        lazyEntryView.setCreationTime(record.getCreationTime());
        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            lazyEntryView.setHits(statistics.getHits());
            lazyEntryView.setExpirationTime(statistics.getExpirationTime());
            lazyEntryView.setLastStoredTime(statistics.getLastStoredTime());
        }
        return lazyEntryView;
    }

}
