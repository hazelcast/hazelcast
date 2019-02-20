/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

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

    private String mapName;
    private String cacheName;
    private boolean enabled = true;
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
        this.mapName = config.mapName;
        this.cacheName = config.cacheName;
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
                + "mapName='" + mapName + '\''
                + ", cacheName='" + cacheName + '\''
                + ", enabled=" + enabled
                + ", capacity=" + capacity
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + '}';
    }

    /**
     * Returns an immutable version of this configuration.
     *
     * @return immutable version of this configuration
     */
    EventJournalConfig getAsReadOnly() {
        return new EventJournalConfigReadOnly(this);
    }

    /**
     * Returns the map name to which this config applies.
     *
     * @return the map name
     */
    public String getMapName() {
        return mapName;
    }

    /**
     * Sets the map name to which this config applies. Map names
     * are also matched by pattern and event journal with map name "default"
     * applies to all maps that do not have more specific event journal configs.
     *
     * @param mapName the map name
     * @return the event journal config
     */
    public EventJournalConfig setMapName(String mapName) {
        this.mapName = mapName;
        return this;
    }

    /**
     * Returns the cache name to which this config applies.
     *
     * @return the cache name
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets the cache name to which this config applies. Cache names
     * are also matched by pattern and event journal with cache name "default"
     * applies to all caches that do not have more specific event journal configs.
     *
     * @param cacheName the cache name
     * @return the event journal config
     */
    public EventJournalConfig setCacheName(String cacheName) {
        this.cacheName = checkHasText(cacheName, "name must contain text");
        return this;
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
    public int getId() {
        return ConfigDataSerializerHook.EVENT_JOURNAL_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeUTF(cacheName);
        out.writeBoolean(enabled);
        out.writeInt(capacity);
        out.writeInt(timeToLiveSeconds);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        cacheName = in.readUTF();
        enabled = in.readBoolean();
        capacity = in.readInt();
        timeToLiveSeconds = in.readInt();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
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
        if (timeToLiveSeconds != that.timeToLiveSeconds) {
            return false;
        }
        if (mapName != null ? !mapName.equals(that.mapName) : that.mapName != null) {
            return false;
        }
        return cacheName != null ? cacheName.equals(that.cacheName) : that.cacheName == null;
    }

    @Override
    public final int hashCode() {
        int result = mapName != null ? mapName.hashCode() : 0;
        result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + capacity;
        result = 31 * result + timeToLiveSeconds;
        return result;
    }

    // not private for testing
    static class EventJournalConfigReadOnly extends EventJournalConfig {
        EventJournalConfigReadOnly(EventJournalConfig config) {
            super(config);
        }

        @Override
        public EventJournalConfig setCapacity(int capacity) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public EventJournalConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public EventJournalConfig setEnabled(boolean enabled) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public EventJournalConfig setMapName(String mapName) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public EventJournalConfig setCacheName(String cacheName) {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
