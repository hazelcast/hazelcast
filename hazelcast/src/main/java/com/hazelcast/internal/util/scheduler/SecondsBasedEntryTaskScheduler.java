/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.util.scheduler;

import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.internal.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Schedule execution of an entry for seconds later.
 * This is similar to a scheduled executor service, but instead of scheduling
 * a execution for a specific millisecond, this service will
 * schedule it with second proximity. For example, if delayMillis is 600 ms,
 * then the entry will be scheduled to execute in 1 second. If delayMillis is 2400,
 * then the entry will be scheduled to execute in 3 seconds. Therefore, delayMillis is
 * ceil-ed to the next second. It gives up exact time scheduling to gain
 * the power of:
 * a) bulk execution of all operations within the same second
 * or
 * b) being able to reschedule (postpone) execution.
 *
 * @param <K> entry key type
 * @param <V> entry value type
 */
public final class SecondsBasedEntryTaskScheduler<K, V> implements EntryTaskScheduler<K, V> {

    /**
     * @see #ceilToSecond(long)
     */
    public static final double FACTOR = 1000d;

    private static final long INITIAL_TIME_MILLIS = Clock.currentTimeMillis();

    private static final Comparator<ScheduledEntry> SCHEDULED_ENTRIES_COMPARATOR = new Comparator<ScheduledEntry>() {
        @Override
        public int compare(ScheduledEntry o1, ScheduledEntry o2) {
            if (o1.getScheduleId() > o2.getScheduleId()) {
                return 1;
            } else if (o1.getScheduleId() < o2.getScheduleId()) {
                return -1;
            }
            return 0;
        }
    };

    /** Map from entry key to the scheduler responsible for this key */
    private HashMap<K, PerKeyScheduler> keys = new HashMap<>();
    /** Map from second to the group of entries to be processed in this second */
    private HashMap<Integer, ScheduledGroup> groups = new HashMap<>();

    private final AtomicLong uniqueIdGenerator = new AtomicLong();
    private final Object mutex = new Object();

    private final TaskScheduler taskScheduler;
    private final ScheduledEntryProcessor<K, V> entryProcessor;
    private final ScheduleType scheduleType;

    SecondsBasedEntryTaskScheduler(TaskScheduler taskScheduler,
                                   ScheduledEntryProcessor<K, V> entryProcessor, ScheduleType scheduleType) {
        this.taskScheduler = taskScheduler;
        this.entryProcessor = entryProcessor;
        this.scheduleType = scheduleType;
    }

    @Override
    public boolean schedule(long delayMillis, K key, V value) {
        int delaySeconds = ceilToSecond(delayMillis);
        int second = findRelativeSecond(delayMillis);
        long id = uniqueIdGenerator.incrementAndGet();
        synchronized (mutex) {
            ScheduledEntry<K, V> entry = new ScheduledEntry<>(key, value, delayMillis, delaySeconds, id);
            PerKeyScheduler keyScheduler = keys.get(key);
            if (keyScheduler == null) {
                switch (scheduleType) {
                    case POSTPONE: keyScheduler = new PerKeyPostponeScheduler(key); break;
                    case FOR_EACH: keyScheduler = new PerKeyForEachScheduler(key); break;
                    default: throw new RuntimeException("Undefined schedule type.");
                }
                keys.put(key, keyScheduler);
            }
            ScheduledGroup group = groups.get(second);
            if (group == null) {
                Runnable groupExecutor = () -> executeGroup(second);
                ScheduledFuture executorFuture = taskScheduler.schedule(groupExecutor, delaySeconds, TimeUnit.SECONDS);
                group = new ScheduledGroup(second, executorFuture);
                groups.put(second, group);
            }
            return keyScheduler.schedule(entry, group);
        }
    }

    private void executeGroup(int second) {
        List<ScheduledEntry<K, V>> entries;
        synchronized (mutex) {
            ScheduledGroup group = groups.remove(second);
            if (group == null) {
                // group removed in meantime
                return;
            }
            entries = new ArrayList<>(group.listEntries());
            for (ScheduledEntry<K, V> entry : entries) {
                keys.get(entry.getKey()).executed(entry);
            }
        }
        entries.sort(SCHEDULED_ENTRIES_COMPARATOR);
        entryProcessor.process(this, entries);
    }

    @Override
    public ScheduledEntry<K, V> get(K key) {
        synchronized (mutex) {
            PerKeyScheduler keyScheduler = keys.get(key);
            return keyScheduler != null ? keyScheduler.get() : null;
        }
    }

    @Override
    public ScheduledEntry<K, V> cancel(K key) {
        synchronized (mutex) {
            PerKeyScheduler keyScheduler = keys.get(key);
            return keyScheduler != null ? keyScheduler.cancel() : null;
        }
    }

    @Override
    public int cancelIfExists(K key, V value) {
        synchronized (mutex) {
            PerKeyScheduler keyScheduler = keys.get(key);
            return keyScheduler != null ? keyScheduler.cancelIfExists(value) : 0;
        }
    }

    /**
     * Decides how to schedule entries for a given key, and keeps track of what have been scheduled for the key.
     */
    private abstract class PerKeyScheduler {
        abstract boolean schedule(ScheduledEntry<K, V> entry, ScheduledGroup group);
        abstract ScheduledEntry<K, V> get();
        abstract ScheduledEntry<K, V> cancel();
        abstract int cancelIfExists(V value);
        abstract void executed(ScheduledEntry<K, V> entry);
    }

    /**
     * Manages scheduling where there should be just one entry per key, and any subsequent scheduling request
     * for this key just postpones the execution, according to the delay specified in new request.
     * @see ScheduleType#POSTPONE
     */
    private final class PerKeyPostponeScheduler extends PerKeyScheduler {
        final K key;
        Long id;
        ScheduledGroup group;

        PerKeyPostponeScheduler(K key) {
            this.key = key;
        }

        boolean schedule(ScheduledEntry<K, V> newEntry, ScheduledGroup newGroup) {
            if (newGroup == group) {
                // no reschedule if in the same second
                return false;
            }
            if (group != null) {
                // unschedule previous entry
                group.removeEntry(id);
            }
            id = newEntry.getScheduleId();
            group = newGroup;
            newGroup.addEntry(id, newEntry);
            return true;
        }

        ScheduledEntry<K, V> get() {
            return group.getEntry(id);
        }

        ScheduledEntry<K, V> cancel() {
            ScheduledEntry<K, V> entry = group.removeEntry(id);
            keys.remove(key);
            return entry;
        }

        int cancelIfExists(V value) {
            ScheduledEntry<K, V> entry = group.getEntry(id);
            if (Objects.equals(entry.getValue(), value)) {
                group.removeEntry(id);
                keys.remove(key);
                return 1;
            }
            return 0;
        }

        void executed(ScheduledEntry<K, V> entry) {
            assert entry.getScheduleId() == id;
            // no need to remove entry from group, whole group is being executed and will be removed
            keys.remove(key);
        }
    }

    /**
     * Manages scheduling where each scheduled entry for given key should be executed when its time comes.
     * @see ScheduleType#FOR_EACH
     */
    private final class PerKeyForEachScheduler extends PerKeyScheduler {
        final K key;
        final Map<Long, ScheduledGroup> idToGroupMap = new HashMap<>();

        PerKeyForEachScheduler(K key) {
            this.key = key;
        }

        boolean schedule(ScheduledEntry<K, V> entry, ScheduledGroup group) {
            Long id = entry.getScheduleId();
            idToGroupMap.put(id, group);
            group.addEntry(id, entry);
            return true;
        }

        ScheduledEntry<K, V> get() {
            ScheduledEntry<K, V> entry = null;
            for (Map.Entry<Long, ScheduledGroup> idToGroup : idToGroupMap.entrySet()) {
                Long id = idToGroup.getKey();
                ScheduledGroup group = idToGroup.getValue();
                entry = group.getEntry(id);
            }
            assert entry != null;
            return entry;
        }

        ScheduledEntry<K, V> cancel() {
            ScheduledEntry<K, V> entry = null;
            for (Map.Entry<Long, ScheduledGroup> idToGroup : idToGroupMap.entrySet()) {
                Long id = idToGroup.getKey();
                ScheduledGroup group = idToGroup.getValue();
                entry = group.removeEntry(id);
            }
            keys.remove(key);
            assert entry != null;
            return entry;
        }

        int cancelIfExists(V value) {
            int cancelled = 0;
            Iterator<Map.Entry<Long, ScheduledGroup>> iterator = idToGroupMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, ScheduledGroup> idToGroup = iterator.next();
                Long id = idToGroup.getKey();
                ScheduledGroup group = idToGroup.getValue();
                ScheduledEntry<K, V> entry = group.getEntry(id);
                if (Objects.equals(entry.getValue(), value)) {
                    group.removeEntry(id);
                    iterator.remove();
                    cancelled++;
                }
            }
            if (idToGroupMap.isEmpty()) {
                keys.remove(key);
            }
            return cancelled;
        }

        void executed(ScheduledEntry<K, V> entry) {
            idToGroupMap.remove(entry.getScheduleId());
            // no need to remove entry from group, whole group is being executed and will be removed
            if (idToGroupMap.isEmpty()) {
                keys.remove(key);
            }
        }
    }

    /**
     * Group of entries scheduled for execution in the same second.
     */
    private final class ScheduledGroup {
        final int second;
        final ScheduledFuture executor;
        final Map<Long, ScheduledEntry<K, V>> idToEntryMap = new HashMap<>();

        private ScheduledGroup(int second, ScheduledFuture executor) {
            this.second = second;
            this.executor = executor;
        }

        private void addEntry(Long id, ScheduledEntry<K, V> entry) {
            idToEntryMap.put(id, entry);
        }

        private ScheduledEntry<K, V> getEntry(Long id) {
            return idToEntryMap.get(id);
        }

        private Collection<ScheduledEntry<K, V>> listEntries() {
            return idToEntryMap.values();
        }

        private int countEntries() {
            return idToEntryMap.size();
        }

        private ScheduledEntry<K, V> removeEntry(Long id) {
            ScheduledEntry<K, V> entry = idToEntryMap.remove(id);
            if (idToEntryMap.isEmpty()) {
                executor.cancel(false);
                groups.remove(second);
            }
            return entry;
        }
    }

    @Override
    public void cancelAll() {
        synchronized (mutex) {
            for (ScheduledGroup group : groups.values()) {
                group.executor.cancel(false);
            }
            groups.clear();
            keys.clear();
        }
    }

    @Override
    public String toString() {
        return "EntryTaskScheduler{"
                + "numberOfEntries="
                + size()
                + ", numberOfKeys="
                + keys.size()
                + ", numberOfGroups="
                + groups.size()
                + '}';
    }

    /**
     * Returns number of scheduled entries.
     */
    // exposed for testing
    int size() {
        synchronized (mutex) {
            int size = 0;
            for (ScheduledGroup group : groups.values()) {
                size += group.countEntries();
            }
            return size;
        }
    }

    /**
     * Returns {@code true} if there are no scheduled entries.
     */
    // exposed for testing
    public boolean isEmpty() {
        synchronized (mutex) {
            // check every collection for emptiness to make sure no memory is leaking
            return groups.isEmpty() && keys.isEmpty();
        }
    }

    /**
     * Returns the duration in seconds between the time this class was loaded and now+{@code delayMillis}
     */
    // exposed for testing
    static int findRelativeSecond(long delayMillis) {
        long now = Clock.currentTimeMillis();
        long d = (now + delayMillis - INITIAL_TIME_MILLIS);
        return ceilToSecond(d);
    }

    private static int ceilToSecond(long delayMillis) {
        return (int) Math.ceil(delayMillis / FACTOR);
    }
}
