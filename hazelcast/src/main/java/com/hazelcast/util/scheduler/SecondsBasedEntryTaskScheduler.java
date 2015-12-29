/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.scheduler;

import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
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
final class SecondsBasedEntryTaskScheduler<K, V> implements EntryTaskScheduler<K, V> {

    public static final int INITIAL_CAPACITY = 10;

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

    private final ConcurrentMap<Object, Integer> secondsOfKeys = new ConcurrentHashMap<Object, Integer>(1000);
    private final ConcurrentMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>> scheduledEntries
            = new ConcurrentHashMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>>(1000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledEntryProcessor<K, V> entryProcessor;
    private final ScheduleType scheduleType;
    private final ConcurrentMap<Integer, ScheduledFuture> scheduledTaskMap
            = new ConcurrentHashMap<Integer, ScheduledFuture>(1000);
    private final AtomicLong uniqueIdGenerator = new AtomicLong();

    SecondsBasedEntryTaskScheduler(ScheduledExecutorService scheduledExecutorService,
                                   ScheduledEntryProcessor<K, V> entryProcessor, ScheduleType scheduleType) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.entryProcessor = entryProcessor;
        this.scheduleType = scheduleType;
    }

    @Override
    public boolean schedule(long delayMillis, K key, V value) {
        if (scheduleType.equals(ScheduleType.POSTPONE)) {
            return schedulePostponeEntry(delayMillis, key, value);
        } else if (scheduleType.equals(ScheduleType.SCHEDULE_IF_NEW)) {
            return scheduleIfNew(delayMillis, key, value);
        } else if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return scheduleEntry(delayMillis, key, value);
        } else {
            throw new RuntimeException("Undefined schedule type.");
        }
    }

    @Override
    public Set<K> flush(Set<K> keys) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return flushByCompositeKeys(keys);
        }
        List<ScheduledEntry<K, V>> res = new ArrayList<ScheduledEntry<K, V>>(keys.size());
        Set<K> processedKeys = new HashSet<K>();
        for (K key : keys) {
            final Integer second = secondsOfKeys.remove(key);
            if (second != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
                if (entries != null) {
                    processedKeys.add(key);
                    res.add(entries.remove(key));
                }
            }
        }
        entryProcessor.process(this, sortForEntryProcessing(res));
        return processedKeys;
    }

    private Set flushByCompositeKeys(Set keys) {
        List<ScheduledEntry<K, V>> res = new ArrayList<ScheduledEntry<K, V>>(keys.size());
        Set<CompositeKey> candidateKeys = new HashSet<CompositeKey>();
        Set processedKeys = new HashSet();
        for (Object key : keys) {
            for (Object skey : secondsOfKeys.keySet()) {
                CompositeKey compositeKey = (CompositeKey) skey;
                if (key.equals(compositeKey.getKey())) {
                    candidateKeys.add(compositeKey);
                }
            }
        }
        for (CompositeKey compositeKey : candidateKeys) {
            final Integer second = secondsOfKeys.remove(compositeKey);
            if (second != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
                if (entries != null) {
                    res.add(entries.remove(compositeKey));
                    processedKeys.add(compositeKey.getKey());
                }
            }

        }
        entryProcessor.process(this, sortForEntryProcessing(res));
        return processedKeys;
    }

    @Override
    public ScheduledEntry<K, V> cancel(K key) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return cancelByCompositeKey(key);
        }
        final Integer second = secondsOfKeys.remove(key);
        if (second == null) {
            return null;
        }
        final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
        if (entries == null) {
            return null;
        }
        return cancelAndCleanUpIfEmpty(second, entries, key);
    }

    @Override
    public int cancelIfExists(K key, V value) {
        final ScheduledEntry<K, V> scheduledEntry = new ScheduledEntry<K, V>(key, value, 0, 0, 0);

        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return cancelByCompositeKey(key, scheduledEntry);
        }

        final Integer second = secondsOfKeys.remove(key);
        if (second == null) {
            return 0;
        }
        final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
        if (entries == null) {
            return 0;
        }

        return cancelAndCleanUpIfEmpty(second, entries, key, scheduledEntry) ? 1 : 0;
    }

    @Override
    public ScheduledEntry<K, V> get(K key) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return getByCompositeKey(key);
        }
        final Integer second = secondsOfKeys.get(key);
        if (second != null) {
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
            if (entries != null) {
                return entries.get(key);
            }
        }
        return null;
    }

    private ScheduledEntry<K, V> cancelByCompositeKey(K key) {
        Set<CompositeKey> candidateKeys = getCompositeKeys(key);

        ScheduledEntry<K, V> result = null;
        for (CompositeKey compositeKey : candidateKeys) {
            final Integer second = secondsOfKeys.remove(compositeKey);
            if (second == null) {
                continue;
            }
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
            if (entries == null) {
                continue;
            }
            result = cancelAndCleanUpIfEmpty(second, entries, compositeKey);
        }
        return result;
    }

    private int cancelByCompositeKey(K key, final ScheduledEntry<K, V> entryToRemove) {
        int cancelled = 0;
        for (CompositeKey compositeKey : getCompositeKeys(key)) {
            final Integer second = secondsOfKeys.remove(compositeKey);
            if (second == null) {
                continue;
            }
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
            if (entries == null) {
                continue;
            }

            if (cancelAndCleanUpIfEmpty(second, entries, compositeKey, entryToRemove)) {
                cancelled++;
            }
        }
        return cancelled;
    }

    private Set<CompositeKey> getCompositeKeys(K key) {
        final Set<CompositeKey> candidateKeys = new HashSet<CompositeKey>();
        for (Object keyObj : secondsOfKeys.keySet()) {
            CompositeKey compositeKey = (CompositeKey) keyObj;
            if (compositeKey.getKey().equals(key)) {
                candidateKeys.add(compositeKey);
            }
        }
        return candidateKeys;
    }

    public ScheduledEntry<K, V> getByCompositeKey(K key) {
        final Set<CompositeKey> candidateKeys = getCompositeKeys(key);
        ScheduledEntry<K, V> result = null;
        for (CompositeKey compositeKey : candidateKeys) {
            final Integer second = secondsOfKeys.get(compositeKey);
            if (second != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
                if (entries != null) {
                    result = entries.get(compositeKey);
                }
            }
        }
        return result;
    }

    private boolean schedulePostponeEntry(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        final Integer existingSecond = secondsOfKeys.put(key, newSecond);
        if (existingSecond != null) {
            if (existingSecond.equals(newSecond)) {
                return false;
            }
            removeKeyFromSecond(key, existingSecond);
        }
        final long id = uniqueIdGenerator.incrementAndGet();
        doSchedule(key, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds, id), newSecond);
        return true;
    }

    private boolean scheduleEntry(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        final long id = uniqueIdGenerator.incrementAndGet();
        Object compositeKey = new CompositeKey(key, id);
        secondsOfKeys.put(compositeKey, newSecond);
        doSchedule(compositeKey, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds, id), newSecond);
        return true;
    }

    private boolean scheduleIfNew(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        if (secondsOfKeys.putIfAbsent(key, newSecond) != null) {
            return false;
        }
        final long id = uniqueIdGenerator.incrementAndGet();
        doSchedule(key, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds, id), newSecond);
        return true;
    }

    private int findRelativeSecond(long delayMillis) {
        long now = Clock.currentTimeMillis();
        long d = (now + delayMillis - INITIAL_TIME_MILLIS);
        return ceilToSecond(d);
    }

    private int ceilToSecond(long delayMillis) {
        return (int) Math.ceil(delayMillis / FACTOR);
    }

    private void doSchedule(Object mapKey, ScheduledEntry<K, V> entry, Integer second) {
        ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
        boolean shouldSchedule = false;
        if (entries == null) {
            entries = new ConcurrentHashMap<Object, ScheduledEntry<K, V>>(INITIAL_CAPACITY);
            ConcurrentMap<Object, ScheduledEntry<K, V>> existingScheduleKeys
                    = scheduledEntries.putIfAbsent(second, entries);
            if (existingScheduleKeys != null) {
                entries = existingScheduleKeys;
            } else {
                // we created the second
                // so we will schedule its execution
                shouldSchedule = true;
            }
        }
        entries.put(mapKey, entry);
        if (shouldSchedule) {
            schedule(second, entry.getActualDelaySeconds());
        }
    }

    private void removeKeyFromSecond(Object key, Integer existingSecond) {
        ConcurrentMap<Object, ScheduledEntry<K, V>> scheduledKeys = scheduledEntries.get(existingSecond);
        if (scheduledKeys != null) {
            cancelAndCleanUpIfEmpty(existingSecond, scheduledKeys, key);
        }
    }


    /**
     * Removes the entry from being scheduled to be evicted.
     *
     * Cleans up parent container (second -> entries map) if it doesn't hold anymore items this second.
     *
     * Cancels associated scheduler (second -> scheduler map ) if there are no more items to remove for this second.
     *
     * Returns associated scheduled entry.
     *
     * @param second second at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     * @param key entry key
     * @return associated scheduled entry
     */
    private ScheduledEntry<K, V> cancelAndCleanUpIfEmpty(Integer second, ConcurrentMap<Object, ScheduledEntry<K, V>> entries,
                                                         Object key) {
        final ScheduledEntry<K, V> result = entries.remove(key);
        cleanUpScheduledFuturesIfEmpty(second, entries);
        return result;
    }

    /**
     * Removes the entry if it exists from being scheduled to be evicted.
     *
     * Cleans up parent container (second -> entries map) if it doesn't hold anymore items this second.
     *
     * Cancels associated scheduler (second -> scheduler map ) if there are no more items to remove for this second.
     *
     * Returns associated scheduled entry.
     *
     * @param second second at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     * @param key entry key
     * @param entryToRemove entry value that is expected to exist in the map
     * @return true if entryToRemove exists in the map and removed
     */
    private boolean cancelAndCleanUpIfEmpty(Integer second, ConcurrentMap<Object, ScheduledEntry<K, V>> entries, Object key,
                                            ScheduledEntry<K, V> entryToRemove) {
        final boolean removed = entries.remove(key, entryToRemove);
        cleanUpScheduledFuturesIfEmpty(second, entries);
        return removed;
    }

    /**
     * Cancels the scheduled future and removes the entries map for the given second If no entries are left
     *
     * Cleans up parent container (second -> entries map) if it doesn't hold anymore items this second.
     *
     * Cancels associated scheduler (second -> scheduler map ) if there are no more items to remove for this second.
     *
     * @param second second at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     */
    private void cleanUpScheduledFuturesIfEmpty(Integer second, ConcurrentMap<Object, ScheduledEntry<K, V>> entries) {
        if (entries.isEmpty()) {
            scheduledEntries.remove(second);

            ScheduledFuture removedFeature = scheduledTaskMap.remove(second);
            if (removedFeature != null) {
                removedFeature.cancel(false);
            }
        }
    }

    private void schedule(final Integer second, final int delaySeconds) {
        EntryProcessorExecutor command = new EntryProcessorExecutor(second);
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(command, delaySeconds, TimeUnit.SECONDS);
        scheduledTaskMap.put(second, scheduledFuture);
    }

    private final class EntryProcessorExecutor implements Runnable {
        private final Integer second;

        private EntryProcessorExecutor(Integer second) {
            this.second = second;
        }

        @Override
        public void run() {
            scheduledTaskMap.remove(second);
            final Map<Object, ScheduledEntry<K, V>> entries = scheduledEntries.remove(second);
            if (entries == null || entries.isEmpty()) {
                return;
            }
            List<ScheduledEntry<K, V>> values = new ArrayList<ScheduledEntry<K, V>>(entries.size());
            for (Map.Entry<Object, ScheduledEntry<K, V>> entry : entries.entrySet()) {
                Integer removed = secondsOfKeys.remove(entry.getKey());
                if (removed != null) {
                    values.add(entry.getValue());
                }
            }
            //sort entries asc by schedule times and send to processor.
            entryProcessor.process(SecondsBasedEntryTaskScheduler.this, sortForEntryProcessing(values));
        }
    }

    private List<ScheduledEntry<K, V>> sortForEntryProcessing(List<ScheduledEntry<K, V>> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        }

        Collections.sort(coll, SCHEDULED_ENTRIES_COMPARATOR);
        return coll;
    }


    @Override
    public int size() {
        return secondsOfKeys.size();
    }

    public void cancelAll() {
        secondsOfKeys.clear();
        scheduledEntries.clear();
        for (ScheduledFuture task : scheduledTaskMap.values()) {
            task.cancel(false);
        }
        scheduledTaskMap.clear();
    }

    @Override
    public String toString() {
        return "EntryTaskScheduler{"
                + "secondsOfKeys="
                + secondsOfKeys.size()
                + ", scheduledEntries ["
                + scheduledEntries.size()
                + "] ="
                + scheduledEntries.keySet()
                + '}';
    }
}
