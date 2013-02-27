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

import com.hazelcast.util.Clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
 */

class SecondsBasedEntryTaskScheduler<K, V> implements EntryTaskScheduler<K, V> {

    private static final long initialTimeMillis = Clock.currentTimeMillis();

    private final ConcurrentMap<K, Integer> secondsOfKeys = new ConcurrentHashMap<K, Integer>(1000);
    private final ConcurrentMap<Integer, ConcurrentMap<K, ScheduledEntry<K, V>>> scheduledEntries = new ConcurrentHashMap<Integer, ConcurrentMap<K, ScheduledEntry<K, V>>>(1000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledEntryProcessor entryProcessor;
    private final boolean postponesSchedule;

    SecondsBasedEntryTaskScheduler(ScheduledExecutorService scheduledExecutorService, ScheduledEntryProcessor entryProcessor, boolean postponesSchedule) {
            this.scheduledExecutorService = scheduledExecutorService;
            this.entryProcessor = entryProcessor;
            this.postponesSchedule = postponesSchedule;
    }

    public boolean schedule(long delayMillis, K key, V value) {
        if (postponesSchedule) {
            return scheduleEntry(delayMillis, key, value);
        } else {
            return scheduleIfNew(delayMillis, key, value);
        }
    }

    public ScheduledEntry<K, V> cancel(K key) {
        final Integer second = secondsOfKeys.remove(key);
        if (second != null) {
            final ConcurrentMap<K, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
            if (entries != null) {
                return entries.remove(key);
            }
        }
        return null;
    }

    private boolean scheduleEntry(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        final Integer existingSecond = secondsOfKeys.put(key, newSecond);
        if (existingSecond != null) {
            if (existingSecond.equals(newSecond)){
                System.out.println("same second existing:"+existingSecond+ " new:"+newSecond);
                return false;
            }
            removeKeyFromSecond(key, existingSecond);
        }
        doSchedule(new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds), newSecond);
        return true;
    }

    private boolean scheduleIfNew(long delayMillis, K key, V value) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        if (secondsOfKeys.putIfAbsent(key, newSecond) != null) return false;
        doSchedule(new ScheduledEntry<K, V>(key, value, delayMillis, delaySeconds), newSecond);
        return true;
    }

    private int findRelativeSecond(long delayMillis) {
        long now = Clock.currentTimeMillis();
        long d = (now + delayMillis - initialTimeMillis);
        return ceilToSecond(d);
    }

    private int ceilToSecond(long delayMillis) {
        return (int) Math.ceil(delayMillis / 1000d);
    }

    private void doSchedule(ScheduledEntry<K, V> entry, Integer second) {
        ConcurrentMap<K, ScheduledEntry<K, V>> entries = scheduledEntries.get(second);
        boolean shouldSchedule = false;
        if (entries == null) {
            entries = new ConcurrentHashMap<K, ScheduledEntry<K, V>>(10);
            ConcurrentMap<K, ScheduledEntry<K, V>> existingScheduleKeys = scheduledEntries.putIfAbsent(second, entries);
            if (existingScheduleKeys != null) {
                entries = existingScheduleKeys;
            } else {
                // we created the second
                // so we will schedule its execution
                shouldSchedule = true;
            }
        }
        entries.put(entry.getKey(), entry);
        if (shouldSchedule) {
            schedule(second, entry.getActualDelaySeconds());
        }
    }

    private void removeKeyFromSecond(K key, Integer existingSecond) {
        ConcurrentMap<K, ScheduledEntry<K, V>> scheduledKeys = scheduledEntries.get(existingSecond);
        if (scheduledKeys != null) {
            scheduledKeys.remove(key);
            System.out.println("removed key:"+key);
        }
    }

    private void schedule(final Integer second, final int delaySeconds) {
        scheduledExecutorService.schedule(new EntryProcessorExecutor(second), delaySeconds, TimeUnit.SECONDS);
    }

    private class EntryProcessorExecutor implements Runnable {
        private final Integer second;

        private EntryProcessorExecutor(Integer second) {
            this.second = second;
        }

        public void run() {
            final ConcurrentMap<K, ScheduledEntry<K, V>> entries = scheduledEntries.remove(second);
            if (entries == null || entries.isEmpty()) return;
                for (K key : entries.keySet()) {
                    secondsOfKeys.remove(key);
                }
            entryProcessor.process(SecondsBasedEntryTaskScheduler.this, entries.values());
        }
    }

    public void cancelAll() {
        secondsOfKeys.clear();
        scheduledEntries.clear();
    }

    @Override
    public String toString() {
        return "EntryTaskScheduler{" +
                "secondsOfKeys=" + secondsOfKeys.size() +
                ", scheduledEntries [" + scheduledEntries.size() + "] =" + scheduledEntries.keySet() +
                '}';
    }
}
