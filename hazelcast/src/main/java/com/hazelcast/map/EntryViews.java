package com.hazelcast.map;

import com.hazelcast.core.EntryView;

public final class EntryViews {

    private EntryViews() {
    }

    public static <K, V> EntryView<K, V> createNullEntryView(K key) {
        return new NullEntryView<K, V>(key);
    }
}
