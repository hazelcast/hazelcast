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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Entry task execution scheduler, rounding up schedule time to a configurable number of slots per second.
 *
 * This is similar to a scheduled executor service, but instead of scheduling an execution for a specific
 * millisecond, this service will schedule it with configurable granularity.
 *
 * @param <K> entry key type
 * @param <V> entry value type
 */
final class SlotBasedEntryTaskScheduler<K, V> implements EntryTaskScheduler<K, V> {

    public static final int INITIAL_CAPACITY = 10;

    public static final int MILLIS_PER_SECOND = 1000;

    private static final long INITIAL_TIME_MILLIS = Clock.currentTimeMillis();

    private static final int DEFAULT_SLOTS_PER_SECOND = 20;
    private static final int DEFAULT_SLOT_DURATION_MILLIS = MILLIS_PER_SECOND / DEFAULT_SLOTS_PER_SECOND;

    private static final Comparator<ScheduledEntry> SCHEDULED_ENTRIES_COMPARATOR = new Comparator<ScheduledEntry>() {
        @Override
        public int compare(ScheduledEntry o1, ScheduledEntry o2) {
            if (o1.getScheduleStartTimeInNanos() > o2.getScheduleStartTimeInNanos()) {
                return 1;
            } else if (o1.getScheduleStartTimeInNanos() < o2.getScheduleStartTimeInNanos()) {
                return -1;
            }
            return 0;
        }
    };

    // key -> slot #, slot 0 starts at INITIAL_TIME_MILLIS and each subsequent slot starts at
    // INITIAL_TIME_MILLIS + (slotNumber * millisPerSlot)
    private final ConcurrentMap<Object, Integer> keyToSlot = new ConcurrentHashMap<Object, Integer>(1000);
    private final ConcurrentMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>> slotToScheduledEntry
            = new ConcurrentHashMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>>(1000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledEntryProcessor<K, V> entryProcessor;
    private final ScheduleType scheduleType;
    private final ConcurrentMap<Integer, ScheduledFuture> slotToFuture
            = new ConcurrentHashMap<Integer, ScheduledFuture>(1000);
    private final int slotsPerSecond;
    private final int millisPerSlot;
    private final ILogger logger = Logger.getLogger(SlotBasedEntryTaskScheduler.class);

    SlotBasedEntryTaskScheduler(ScheduledExecutorService scheduledExecutorService,
                                ScheduledEntryProcessor<K, V> entryProcessor, ScheduleType scheduleType) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.entryProcessor = entryProcessor;
        this.scheduleType = scheduleType;
        this.slotsPerSecond = DEFAULT_SLOTS_PER_SECOND;
        this.millisPerSlot = DEFAULT_SLOT_DURATION_MILLIS;
    }

    SlotBasedEntryTaskScheduler(ScheduledExecutorService scheduledExecutorService,
                                ScheduledEntryProcessor<K, V> entryProcessor, ScheduleType scheduleType,
                                int slotsPerSecond) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.entryProcessor = entryProcessor;
        this.scheduleType = scheduleType;
        this.slotsPerSecond = slotsPerSecond;
        this.millisPerSlot = MILLIS_PER_SECOND / slotsPerSecond;
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
            return flushByTimeKeys(keys);
        }
        Set<ScheduledEntry<K, V>> res = new HashSet<ScheduledEntry<K, V>>(keys.size());
        Set<K> processedKeys = new HashSet<K>();
        for (K key : keys) {
            final Integer slot = keyToSlot.remove(key);
            if (slot != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
                if (entries != null) {
                    processedKeys.add(key);
                    res.add(entries.remove(key));
                }
            }
        }
        entryProcessor.process(this, sortForEntryProcessing(res));
        return processedKeys;
    }

    private Set flushByTimeKeys(Set keys) {
        Set<ScheduledEntry<K, V>> res = new HashSet<ScheduledEntry<K, V>>(keys.size());
        Set<TimeKey> candidateKeys = new HashSet<TimeKey>();
        Set processedKeys = new HashSet();
        for (Object key : keys) {
            for (Object skey : keyToSlot.keySet()) {
                TimeKey timeKey = (TimeKey) skey;
                if (key.equals(timeKey.getKey())) {
                    candidateKeys.add(timeKey);
                }
            }
        }
        for (TimeKey timeKey : candidateKeys) {
            final Integer slot = keyToSlot.remove(timeKey);
            if (slot != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
                if (entries != null) {
                    res.add(entries.remove(timeKey));
                    processedKeys.add(timeKey.getKey());
                }
            }

        }
        entryProcessor.process(this, sortForEntryProcessing(res));
        return processedKeys;
    }

    @Override
    public ScheduledEntry<K, V> cancel(K key) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return cancelByTimeKey(key);
        }
        final Integer slot = keyToSlot.remove(key);
        if (slot == null) {
            return null;
        }
        final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
        if (entries == null) {
            return null;
        }
        return cancelAndCleanUpIfEmpty(slot, entries, key);
    }

    @Override
    public int cancelIfExists(K key, V value) {
        final ScheduledEntry<K, V> scheduledEntry = new ScheduledEntry<K, V>(key, value, 0, 0);

        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return cancelByTimeKey(key, scheduledEntry);
        }

        final Integer slot = keyToSlot.remove(key);
        if (slot == null) {
            return 0;
        }
        final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
        if (entries == null) {
            return 0;
        }

        return cancelAndCleanUpIfEmpty(slot, entries, key, scheduledEntry) ? 1 : 0;
    }

    @Override
    public ScheduledEntry<K, V> get(K key) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return getByTimeKey(key);
        }
        final Integer slot = keyToSlot.get(key);
        if (slot != null) {
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
            if (entries != null) {
                return entries.get(key);
            }
        }
        return null;
    }

    private ScheduledEntry<K, V> cancelByTimeKey(K key) {
        Set<TimeKey> candidateKeys = getTimeKeys(key);

        ScheduledEntry<K, V> result = null;
        for (TimeKey timeKey : candidateKeys) {
            final Integer slot = keyToSlot.remove(timeKey);
            if (slot == null) {
                continue;
            }
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
            if (entries == null) {
                continue;
            }
            result = cancelAndCleanUpIfEmpty(slot, entries, timeKey);
        }
        return result;
    }

    private int cancelByTimeKey(K key, final ScheduledEntry<K, V> entryToRemove) {
        int cancelled = 0;
        for (TimeKey timeKey : getTimeKeys(key)) {
            final Integer slot = keyToSlot.remove(timeKey);
            if (slot == null) {
                continue;
            }
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
            if (entries == null) {
                continue;
            }

            if (cancelAndCleanUpIfEmpty(slot, entries, timeKey, entryToRemove)) {
                cancelled++;
            }
        }
        return cancelled;
    }

    private Set<TimeKey> getTimeKeys(K key) {
        final Set<TimeKey> candidateKeys = new HashSet<TimeKey>();
        for (Object timeKeyObj : keyToSlot.keySet()) {
            TimeKey timeKey = (TimeKey) timeKeyObj;
            if (timeKey.getKey().equals(key)) {
                candidateKeys.add(timeKey);
            }
        }
        return candidateKeys;
    }

    public ScheduledEntry<K, V> getByTimeKey(K key) {
        final Set<TimeKey> candidateKeys = getTimeKeys(key);
        ScheduledEntry<K, V> result = null;
        for (TimeKey timeKey : candidateKeys) {
            final Integer slot = keyToSlot.get(timeKey);
            if (slot != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
                if (entries != null) {
                    result = entries.get(timeKey);
                }
            }
        }
        return result;
    }

    private boolean schedulePostponeEntry(long delayMillis, K key, V value) {
        final int actualDelayMillis = ceilToUnit(delayMillis) * millisPerSlot;
        final Integer slot = findSlot(delayMillis);
        final Integer existingSlot = keyToSlot.put(key, slot);
        if (existingSlot != null) {
            if (existingSlot.equals(slot)) {
                return false;
            }
            removeKeyFromSlot(key, existingSlot);
        }
        doSchedule(key, new ScheduledEntry<K, V>(key, value, actualDelayMillis,
                (int) MILLISECONDS.toSeconds(actualDelayMillis)), slot);
        return true;
    }

    private boolean scheduleEntry(long delayMillis, K key, V value) {
        final int actualDelayMillis = ceilToUnit(delayMillis) * millisPerSlot;
        final Integer slot = findSlot(delayMillis);
        long time = System.nanoTime();
        TimeKey timeKey = new TimeKey(key, time);
        keyToSlot.put(timeKey, slot);
        doSchedule(timeKey, new ScheduledEntry<K, V>(key, value, actualDelayMillis,
                (int) MILLISECONDS.toSeconds(actualDelayMillis), time), slot);
        return true;
    }

    private boolean scheduleIfNew(long delayMillis, K key, V value) {
        final int actualDelayMillis = ceilToUnit(delayMillis) * millisPerSlot;
        final Integer slot = findSlot(delayMillis);
        if (keyToSlot.putIfAbsent(key, slot) != null) {
            return false;
        }
        doSchedule(key, new ScheduledEntry<K, V>(key, value, actualDelayMillis,
                (int) MILLISECONDS.toSeconds(actualDelayMillis)), slot);
        return true;
    }

    // find slot number based on this scheduler's reference time INITIAL_TIME_MILLIS
    private int findSlot(long delayMillis) {
        long now = Clock.currentTimeMillis();
        long d = (now + delayMillis - INITIAL_TIME_MILLIS);
        return ceilToUnit(d);
    }

    // round to nearest unit up
    int ceilToUnit(long delayMillis) {
        return (int) Math.ceil((double) delayMillis / millisPerSlot);
    }

    private void doSchedule(Object mapKey, ScheduledEntry<K, V> entry, Integer slot) {
        ConcurrentMap<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.get(slot);
        boolean shouldSchedule = false;
        if (entries == null) {
            entries = new ConcurrentHashMap<Object, ScheduledEntry<K, V>>(INITIAL_CAPACITY);
            ConcurrentMap<Object, ScheduledEntry<K, V>> existingScheduleKeys
                    = slotToScheduledEntry.putIfAbsent(slot, entries);
            if (existingScheduleKeys != null) {
                entries = existingScheduleKeys;
            } else {
                // we created the slot
                // so we will schedule its execution
                shouldSchedule = true;
            }
        }
        entries.put(mapKey, entry);
        if (shouldSchedule) {
            schedule(slot, entry.getScheduledDelayMillis());
        }
    }

    private void removeKeyFromSlot(Object key, Integer existingSlot) {
        ConcurrentMap<Object, ScheduledEntry<K, V>> scheduledKeys = slotToScheduledEntry.get(existingSlot);
        if (scheduledKeys != null) {
            cancelAndCleanUpIfEmpty(existingSlot, scheduledKeys, key);
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
     * @param slot slot at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     * @param key entry key
     * @return associated scheduled entry
     */
    private ScheduledEntry<K, V> cancelAndCleanUpIfEmpty(Integer slot, ConcurrentMap<Object, ScheduledEntry<K, V>> entries,
                                                         Object key) {
        final ScheduledEntry<K, V> result = entries.remove(key);
        cleanUpScheduledFuturesIfEmpty(slot, entries);
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
     * @param slot slot at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     * @param key entry key
     * @param entryToRemove entry value that is expected to exist in the map
     * @return true if entryToRemove exists in the map and removed
     */
    private boolean cancelAndCleanUpIfEmpty(Integer slot, ConcurrentMap<Object, ScheduledEntry<K, V>> entries, Object key,
                                            ScheduledEntry<K, V> entryToRemove) {
        final boolean removed = entries.remove(key, entryToRemove);
        cleanUpScheduledFuturesIfEmpty(slot, entries);
        return removed;
    }

    /**
     * Cancels the scheduled future and removes the entries map for the given second If no entries are left
     *
     * Cleans up parent container (second -> entries map) if it doesn't hold anymore items this second.
     *
     * Cancels associated scheduler (second -> scheduler map ) if there are no more items to remove for this second.
     *
     * @param slot slot at which this entry was scheduled to be evicted
     * @param entries entries which were already scheduled to be evicted for this second
     */
    private void cleanUpScheduledFuturesIfEmpty(Integer slot, ConcurrentMap<Object, ScheduledEntry<K, V>> entries) {
        if (entries.isEmpty()) {
            slotToScheduledEntry.remove(slot);

            ScheduledFuture removedFeature = slotToFuture.remove(slot);
            if (removedFeature != null) {
                removedFeature.cancel(false);
            }
        }
    }

    // todo here I need the actual delay millis!!!
    // delaySeconds -> actual delay relative to now on which execution will occur
    // second -> the theoretical second on which it is scheduled (yet unclear relative to which starting point in time)
    private void schedule(final Integer slot, final long delayMillis) {
        logger.finest("Scheduling slot " + slot + " in delay millis (from now) " + delayMillis);
        EntryProcessorExecutor command = new EntryProcessorExecutor(slot);
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(command, delayMillis, MILLISECONDS);
        slotToFuture.put(slot, scheduledFuture);
    }

    private final class EntryProcessorExecutor implements Runnable {
        private final Integer slot;

        private EntryProcessorExecutor(Integer slot) {
            this.slot = slot;
        }

        @Override
        public void run() {
            logger.finest("Running slot " + slot);
            slotToFuture.remove(slot);
            final Map<Object, ScheduledEntry<K, V>> entries = slotToScheduledEntry.remove(slot);
            if (entries == null || entries.isEmpty()) {
                return;
            }
            Set<ScheduledEntry<K, V>> values = new HashSet<ScheduledEntry<K, V>>(entries.size());
            for (Map.Entry<Object, ScheduledEntry<K, V>> entry : entries.entrySet()) {
                Integer removed = keyToSlot.remove(entry.getKey());
                if (removed != null) {
                    values.add(entry.getValue());
                }
            }
            //sort entries asc by schedule times and send to processor.
            entryProcessor.process(SlotBasedEntryTaskScheduler.this, sortForEntryProcessing(values));
        }
    }

    private List<ScheduledEntry<K, V>> sortForEntryProcessing(Set<ScheduledEntry<K, V>> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        }

        final List<ScheduledEntry<K, V>> sortedEntries = new ArrayList<ScheduledEntry<K, V>>(coll);
        Collections.sort(sortedEntries, SCHEDULED_ENTRIES_COMPARATOR);

        return sortedEntries;
    }


    @Override
    public int size() {
        return keyToSlot.size();
    }

    public void cancelAll() {
        keyToSlot.clear();
        slotToScheduledEntry.clear();
        for (ScheduledFuture task : slotToFuture.values()) {
            task.cancel(false);
        }
        slotToFuture.clear();
    }

    @Override
    public String toString() {
        return "EntryTaskScheduler{"
                + "keyToSlot="
                + keyToSlot.size()
                + ", slotToScheduledEntry ["
                + slotToScheduledEntry.size()
                + "] ="
                + slotToScheduledEntry.keySet()
                + '}';
    }
}
