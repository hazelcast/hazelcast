package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.journal.EventJournalEventAdapter;

public class EventJournalCacheEventAdapter<K, V> implements EventJournalEventAdapter<K, V, EventJournalCacheEvent<K, V>> {
    @Override
    public K getKey(EventJournalCacheEvent<K, V> e) {
        return e.getKey();
    }

    @Override
    public V getNewValue(EventJournalCacheEvent<K, V> e) {
        return e.getNewValue();
    }

    @Override
    public V getOldValue(EventJournalCacheEvent<K, V> e) {
        return e.getOldValue();
    }

    @Override
    public EventType getType(EventJournalCacheEvent<K, V> e) {
        switch (e.getType()) {
            case CREATED:
                return EventType.ADDED;
            case REMOVED:
                return EventType.REMOVED;
            case UPDATED:
                return EventType.UPDATED;
            case EVICTED:
                return EventType.EVICTED;
            default:
                throw new IllegalArgumentException("Unknown event journal event type " + e.getType());
        }
    }
}
