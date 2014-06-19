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

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Entry wrapper with schedule time information to be used in SecondsBasedEntryTaskScheduler.
 * See SecondsBasedEntryTaskScheduler
 *
 * @param <K> key type of scheduled entry
 * @param <V> value type of scheduled entry
 */
public final class ScheduledEntry<K, V> implements Map.Entry<K, V> {

    private final K key;

    private final V value;

    private final long scheduledDelayMillis;

    private final int actualDelaySeconds;

    private final long scheduleStartTimeInNanos;

    public ScheduledEntry(K key, V value, long scheduledDelayMillis, int actualDelaySeconds) {
        this.key = key;
        this.value = value;
        this.scheduledDelayMillis = scheduledDelayMillis;
        this.actualDelaySeconds = actualDelaySeconds;
        this.scheduleStartTimeInNanos = System.nanoTime();
    }

    public ScheduledEntry(K key, V value, long scheduledDelayMillis, int actualDelaySeconds, long scheduleStartTimeInNanos) {
        this.key = key;
        this.value = value;
        this.scheduledDelayMillis = scheduledDelayMillis;
        this.actualDelaySeconds = actualDelaySeconds;
        this.scheduleStartTimeInNanos = scheduleStartTimeInNanos;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new RuntimeException("Operation is not supported");
    }

    public long getScheduledDelayMillis() {
        return scheduledDelayMillis;
    }

    public int getActualDelaySeconds() {
        return actualDelaySeconds;
    }

    public long getScheduleStartTimeInNanos() {
        return scheduleStartTimeInNanos;
    }

    public long getActualDelayMillis() {
        return TimeUnit.SECONDS.toMillis(actualDelaySeconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScheduledEntry that = (ScheduledEntry) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ScheduledEntry{"
                + "key="
                + key
                + ", value="
                + value
                + ", scheduledDelayMillis="
                + scheduledDelayMillis
                + ", actualDelaySeconds="
                + actualDelaySeconds
                + ", scheduleStartTimeInNanos="
                + scheduleStartTimeInNanos
                + '}';
    }
}
