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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for an event journal. The event journal keeps events related
 * to a specific partition and data structure. For instance, it could keep
 * map add, update, remove, merge events along with the key, old value, new value and so on.
 * This configuration is not tied to a specific data structure and can be reused.
 * <b>NOTE</b>
 * This config is intended to be used with <a href="http://jet.hazelcast.org">Hazelcast Jet</a>
 * and does not expose any features in Hazelcast IMDG.
 */
public class EventJournalConfig implements IdentifiedDataSerializable {

    /**
     * Default value of capacity of the event journal.
     */
    public static final int DEFAULT_CAPACITY = 10 * 1000;
    /**
     * Default value for the time to live property.
     */
    public static final int DEFAULT_TTL_SECONDS = 0;

    private boolean enabled;
    private int capacity = DEFAULT_CAPACITY;
    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    public EventJournalConfig() {
    }

    /**
     * Clones a {@link EventJournalConfig}.
     *
     * @param config the event journal config to clone
     * @throws NullPointerException if the config is null
     */
    public EventJournalConfig(EventJournalConfig config) {
        checkNotNull(config, "config can't be null");
        this.enabled = config.enabled;
        this.capacity = config.capacity;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
    }

    /**
     * Gets the capacity of the event journal. The capacity is the total number of items
     * that the event journal can hold at any moment. The actual number of items
     * contained in the journal can be lower.
     * <p>
     * <b>NOTE</b>
     * The capacity is shared equally between all partitions.
     * This is done by assigning each partition {@code getCapacity() / partitionCount}
     * available slots in the event journal. Because of this, the effective total
     * capacity may be somewhat lower and you must make sure that the
     * configured capacity is at least greater than the partition count.
     *
     * @return the capacity.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of the event journal. The capacity is the total number of items
     * that the event journal can hold at any moment. The actual number of items
     * contained in the journal can be lower.
     * <p>
     * <b>NOTE</b>
     * The capacity is shared equally between all partitions.
     * This is done by assigning each partition {@code getCapacity() / partitionCount}
     * available slots in the event journal. Because of this, the effective total
     * capacity may be somewhat lower and you must make sure that the
     * configured capacity is at least greater than the partition count.
     *
     * @param capacity the capacity.
     * @return the updated config.
     * @throws java.lang.IllegalArgumentException if capacity smaller than 1.
     * @see #getCapacity()
     */
    public EventJournalConfig setCapacity(int capacity) {
        checkPositive(capacity, "capacity can't be smaller than 1");
        this.capacity = capacity;
        return this;
    }

    /**
     * Gets the time to live in seconds.
     *
     * @return the time to live in seconds. Returns 0 the time to live if the items don't expire.
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Sets the time to live in seconds.
     * Time to live is the time the event journal retains items before removing them from the journal.
     * The events are removed on journal read and write actions, not while the journal is idle.
     * <p>
     * Time to live can be disabled by setting timeToLiveSeconds to 0. This means that the
     * events never expire but they can be overwritten when the capacity of the journal is exceeded.
     *
     * @param timeToLiveSeconds the time to live period in seconds
     * @return the updated config
     * @throws IllegalArgumentException if timeToLiveSeconds smaller than 0.
     */
    public EventJournalConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = checkNotNegative(timeToLiveSeconds, "timeToLiveSeconds can't be smaller than 0");
        return this;
    }

    @Override
    public String toString() {
        return "EventJournalConfig{"
                + "enabled=" + enabled
                + ", capacity=" + capacity
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + '}';
    }

    /**
     * Returns if the event journal is enabled.
     *
     * @return {@code true} if the event journal is enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }


    /**
     * Enables or disables the event journal.
     *
     * @param enabled {@code true} if enabled, {@code false} otherwise.
     * @return the updated config.
     */
    public EventJournalConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.EVENT_JOURNAL_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeInt(capacity);
        out.writeInt(timeToLiveSeconds);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        capacity = in.readInt();
        timeToLiveSeconds = in.readInt();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventJournalConfig)) {
            return false;
        }

        EventJournalConfig that = (EventJournalConfig) o;
        if (enabled != that.enabled) {
            return false;
        }
        if (capacity != that.capacity) {
            return false;
        }
        return timeToLiveSeconds == that.timeToLiveSeconds;

    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + capacity;
        result = 31 * result + timeToLiveSeconds;
        return result;
    }
}
