package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordStatistics;

import java.util.concurrent.TimeUnit;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class EntryViews {

    private EntryViews() {
    }

    public static <K, V> EntryView<K, V> createNullEntryView(K key) {
        return new NullEntryView<K, V>(key);
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView(K key, V value, Record record) {
        final TimeUnit unit = TimeUnit.NANOSECONDS;
        final SimpleEntryView simpleEntryView = new SimpleEntryView(key, value);
        simpleEntryView.setCost(record.getCost());
        simpleEntryView.setVersion(record.getVersion());
        simpleEntryView.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());
        simpleEntryView.setLastAccessTime(unit.toMillis(record.getLastAccessTime()));
        simpleEntryView.setLastUpdateTime(unit.toMillis(record.getLastUpdateTime()));
        simpleEntryView.setTtl(unit.toMillis(record.getTtl()));

        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            simpleEntryView.setHits(statistics.getHits());
            simpleEntryView.setCreationTime(unit.toMillis(statistics.getCreationTime()));
            simpleEntryView.setExpirationTime(unit.toMillis(statistics.getExpirationTime()));
            simpleEntryView.setLastStoredTime(unit.toMillis(statistics.getLastStoredTime()));
        }
        return simpleEntryView;
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView() {
        return new SimpleEntryView<K, V>();
    }
}
