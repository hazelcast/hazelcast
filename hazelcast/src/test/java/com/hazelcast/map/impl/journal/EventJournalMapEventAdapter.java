package com.hazelcast.map.impl.journal;

import com.hazelcast.journal.EventJournalEventAdapter;
import com.hazelcast.map.journal.EventJournalMapEvent;

public class EventJournalMapEventAdapter<K, V> implements EventJournalEventAdapter<K, V, EventJournalMapEvent<K, V>> {
    @Override
    public K getKey(EventJournalMapEvent<K, V> e) {
        return e.getKey();
    }

    @Override
    public V getNewValue(EventJournalMapEvent<K, V> e) {
        return e.getNewValue();
    }

    @Override
    public V getOldValue(EventJournalMapEvent<K, V> e) {
        return e.getOldValue();
    }

    @Override
    public EventType getType(EventJournalMapEvent<K, V> e) {
        switch (e.getType()) {
            case ADDED:
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
