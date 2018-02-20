package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.journal.EventJournalEventAdapter;
import com.hazelcast.map.journal.EventJournalMapEvent;

public class EventJournalCacheEventAdapter<K, V> implements EventJournalEventAdapter<K, V, EventJournalCacheEvent<K, V>> {
    @Override
    public K getEventKey(EventJournalCacheEvent<K, V> e) {
        return e.getKey();
    }

    @Override
    public V getEventNewValue(EventJournalCacheEvent<K, V> e) {
        return e.getNewValue();
    }

    @Override
    public V getEventOldValue(EventJournalCacheEvent<K, V> e) {
        return e.getOldValue();
    }

    @Override
    public EventType getEventType(EventJournalCacheEvent<K, V> e) {
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
