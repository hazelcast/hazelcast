package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A write-behind queue which supports write coalescing.
 */
class CoalescedWriteBehindQueue implements WriteBehindQueue<DelayedEntry<Data, Object>> {

    protected final Map<Data, DelayedEntry<Data, Object>> queue;

    public CoalescedWriteBehindQueue() {
        queue = new LinkedHashMap<Data, DelayedEntry<Data, Object>>();
    }

    public CoalescedWriteBehindQueue(Map<Data, DelayedEntry<Data, Object>> queue) {
        this.queue = queue;
    }

    @Override
    public boolean offer(DelayedEntry<Data, Object> delayedEntry) {
        if (delayedEntry == null) {
            return false;
        }
        final Data key = delayedEntry.getKey();
        if (queue.containsKey(key)) {
            queue.remove(key);
        }
        queue.put(key, delayedEntry);
        return true;
    }

    @Override
    public void removeFirst() {
        final Set<Data> keySet = queue.keySet();
        for (Data key : keySet) {
            queue.remove(key);
            break;
        }
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public WriteBehindQueue<DelayedEntry<Data, Object>> getSnapShot() {
        return new CoalescedWriteBehindQueue(queue);
    }

    @Override
    public void addFront(Collection<DelayedEntry<Data, Object>> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final LinkedHashMap<Data, DelayedEntry> newQueue = new LinkedHashMap<Data, DelayedEntry>();
        final Iterator<DelayedEntry<Data, Object>> iterator = collection.iterator();
        while (iterator.hasNext()) {
            final DelayedEntry<Data, Object> next = iterator.next();
            newQueue.put(next.getKey(), next);
        }
        newQueue.putAll(queue);
    }

    @Override
    public void addEnd(Collection<DelayedEntry<Data, Object>> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (DelayedEntry<Data, Object> entry : collection) {
            queue.put(entry.getKey(), entry);
        }
    }

    @Override
    public void removeAll(Collection<DelayedEntry<Data, Object>> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (DelayedEntry<Data, Object> entry : collection) {
            final Data entryKey = entry.getKey();
            final Object entryValue = entry.getValue();
            final DelayedEntry delayedEntry = queue.get(entryKey);
            if (delayedEntry == null) {
                continue;
            }
            final Object value = delayedEntry.getValue();
            if (value == entryValue) {
                queue.remove(entryKey);
            }
        }
    }

    @Override
    public List<DelayedEntry<Data, Object>> removeAll() {
        final List<DelayedEntry<Data, Object>> delayedEntries = asList();
        queue.clear();
        return delayedEntries;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public List<DelayedEntry<Data, Object>> asList() {
        final Collection<DelayedEntry<Data, Object>> values = queue.values();
        return new ArrayList<DelayedEntry<Data, Object>>(values);
    }

    @Override
    public Iterator<DelayedEntry<Data, Object>> iterator() {
        final Collection<DelayedEntry<Data, Object>> values = queue.values();
        return values.iterator();
    }
}
