/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.Clock;

/**
 * Schedule execution of an entry for seconds later.
 * This is kind of like a scheduled executor service but instead of scheduling
 * a execution for a specific millisecond, this service will
 * schedule it with second proximity. If delayMillis is 600 ms for example,
 * then the entry will be scheduled to execute in 1 second. If delayMillis is 2400
 * this the entry will be scheduled to execute in 3 seconds. So delayMillis is
 * ceil-ed to the next second. It gives up from exact time scheduling to gain
 * the power of
 * a) bulk execution of all operations within the same second
 * or
 * b) being able to reschedule (postpone) execution
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
            if (o1.getScheduleStartTimeInNanos() > o2.getScheduleStartTimeInNanos()) {
                return 1;
            } else if (o1.getScheduleStartTimeInNanos() < o2.getScheduleStartTimeInNanos()) {
                return -1;
            }
            return 0;
        }
    };

    private final ConcurrentMap<Object, Integer> secondsOfKeys = new ConcurrentHashMap<Object, Integer>(1000);
    private final ConcurrentMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>> scheduledEntries
            = new ConcurrentHashMap<Integer, ConcurrentMap<Object, ScheduledEntry<K, V>>>(1000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledEntryProcessor entryProcessor;
    private final ScheduleType scheduleType;
    private final ConcurrentMap<Integer, ScheduledFuture> scheduledTaskMap
            = new ConcurrentHashMap<Integer, ScheduledFuture>(1000);

    SecondsBasedEntryTaskScheduler(ScheduledExecutorService scheduledExecutorService,
                                   ScheduledEntryProcessor entryProcessor, ScheduleType scheduleType) {
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
            return flushComparingTimeKeys(keys);
        }
        Set<ScheduledEntry<K, V>> res = new HashSet<ScheduledEntry<K, V>>(keys.size());
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

    private Set flushComparingTimeKeys(Set keys) {
        Set<ScheduledEntry<K, V>> res = new HashSet<ScheduledEntry<K, V>>(keys.size());
        Set<TimeKey> candidateKeys = new HashSet<TimeKey>();
        Set processedKeys = new HashSet();
        for (Object key : keys) {
            for (Object skey : secondsOfKeys.keySet()) {
                TimeKey timeKey = (TimeKey) skey;
                if (key.equals(timeKey.getKey())) {
                    candidateKeys.add(timeKey);
                }
            }
        }
        for (TimeKey timeKey : candidateKeys) {
            final Integer second = secondsOfKeys.remove(timeKey);
            if (second != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
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
        ScheduledEntry<K, V> result;

        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return cancelComparingTimeKey(key);
        }
        final Integer second = secondsOfKeys.remove(key);
        if (second == null) {
            return null;
        }
        final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
        if (entries == null) {
            return null;
        }
        result = cleanUpOnCancel(key, second, entries);
        return result;
    }


    @Override
    public ScheduledEntry<K, V> get(K key) {
        if (scheduleType.equals(ScheduleType.FOR_EACH)) {
            return getComparingTimeKey(key);
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

    public ScheduledEntry<K, V> cancelComparingTimeKey(K key) {
        Set<TimeKey> candidateKeys = new HashSet<TimeKey>();
        for (Object tkey : secondsOfKeys.keySet()) {
            TimeKey timeKey = (TimeKey) tkey;
            if (timeKey.getKey().equals(key)) {
                candidateKeys.add(timeKey);
            }
        }

        ScheduledEntry<K, V> result = null;
        for (TimeKey timeKey : candidateKeys) {
            final Integer second = secondsOfKeys.remove(timeKey);
            if (second == null) {
                continue;
            }
            final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
            if (entries == null) {
                continue;
            }
            result = cleanUpOnCancel(timeKey, second, entries);
        }
        return result;
    }

    public ScheduledEntry<K, V> getComparingTimeKey(K key) {
        Set<TimeKey> candidateKeys = new HashSet<TimeKey>();
        for (Object tkey : secondsOfKeys.keySet()) {
            TimeKey timeKey = (TimeKey) tkey;
            if (timeKey.getKey().equals(key)) {
                candidateKeys.add(timeKey);
            }
        }
        ScheduledEntry<K, V> result = null;
        for (TimeKey timeKey : candidateKeys) {
            final Integer second = secondsOfKeys.get(timeKey);
            if (second != null) {
                final ConcurrentMap<Object, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
                if (entries != null) {
                    result = entries.get(timeKey);
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
        doSchedule(key, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds), newSecond);
        return true;
    }

    private boolean scheduleEntry(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        long time = System.nanoTime();
        TimeKey timeKey = new TimeKey(key, time);
        secondsOfKeys.put(timeKey, newSecond);
        doSchedule(timeKey, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds, time), newSecond);
        return true;
    }

    private boolean scheduleIfNew(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        if (secondsOfKeys.putIfAbsent(key, newSecond) != null) {
            return false;
        }
        doSchedule(key, new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds), newSecond);
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
            cleanUpOnCancel(key, existingSecond, scheduledKeys);
        }
    }


    /**
     * Removes entry from from being evicted.
     *
     * Cleans up parent container (second based hashmap) if it doesn't hold anymore items.
     *
     * Cancels associated scheduler if there are no more items to remove for this second.
     *
     * Returns associated scheduled entry.
     */
    private ScheduledEntry<K, V> cleanUpOnCancel(Object key, Integer second, ConcurrentMap<Object, ScheduledEntry<K,
            V>> entries) {
        final ScheduledEntry<K, V> result = entries.remove(key);
        if (entries.isEmpty()) {
            scheduledEntries.remove(second);

            ScheduledFuture removed = scheduledTaskMap.remove(second);
            if (removed != null) {
                removed.cancel(false);
            }
        }
        return result;
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
            Set<ScheduledEntry<K, V>> values = new HashSet<ScheduledEntry<K, V>>(entries.size());
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
