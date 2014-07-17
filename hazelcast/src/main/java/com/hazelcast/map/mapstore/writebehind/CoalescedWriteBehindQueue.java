package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A write-behind queue which supports write coalescing.
 */
class CoalescedWriteBehindQueue implements WriteBehindQueue<DelayedEntry> {

    protected final Map<Data, DelayedEntry> queue;

    public CoalescedWriteBehindQueue() {
        queue = new LinkedHashMap<Data, DelayedEntry>();
    }

    public CoalescedWriteBehindQueue(Map<Data, DelayedEntry> queue) {
        this.queue = queue;
    }

    @Override
    public boolean offer(DelayedEntry delayedEntry) {
        if (delayedEntry == null) {
            return false;
        }
        final Data key = (Data) delayedEntry.getKey();
        if (queue.containsKey(key)) {
            queue.remove(key);
        }
        queue.put(key, delayedEntry);
        return true;
    }

    @Override
    public DelayedEntry get(DelayedEntry entry) {
        return queue.get(entry.getKey());
    }

    @Override
    public DelayedEntry getFirst() {
        final Iterator<DelayedEntry> iterator = queue.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
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
    public WriteBehindQueue<DelayedEntry> getSnapShot() {
        return new CoalescedWriteBehindQueue(queue);
    }

    @Override
    public void addFront(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final LinkedHashMap<Data, DelayedEntry> newQueue = new LinkedHashMap<Data, DelayedEntry>();
        final Iterator<DelayedEntry> iterator = collection.iterator();
        while (iterator.hasNext()) {
            final DelayedEntry next = iterator.next();
            newQueue.put((Data) next.getKey(), next);
        }
        newQueue.putAll(queue);
    }

    @Override
    public void addEnd(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (DelayedEntry entry : collection) {
            queue.put((Data) entry.getKey(), entry);
        }
    }

    @Override
    public void removeAll(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (DelayedEntry entry : collection) {
            final Data entryKey = (Data) entry.getKey();
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
    public List<DelayedEntry> removeAll() {
        final List<DelayedEntry> delayedEntries = asList();
        queue.clear();
        return delayedEntries;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public List<DelayedEntry> asList() {
        final Collection<DelayedEntry> values = queue.values();
        return new ArrayList<DelayedEntry>(values);
    }

    @Override
    public List<DelayedEntry> filterItems(long now) {
        List<DelayedEntry> delayedEntries = null;
        final Collection<DelayedEntry> values = queue.values();
        for (DelayedEntry e : values) {
            if (delayedEntries == null) {
                delayedEntries = new ArrayList<DelayedEntry>();
            }
            if (e.getStoreTime() <= now) {
                delayedEntries.add(e);
            }
        }
        if (delayedEntries == null) {
            return Collections.emptyList();
        }
        return delayedEntries;
    }
}
