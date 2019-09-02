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
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Contains the configuration for the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 * <p>
 * The RingBuffer is a replicated but not partitioned data-structure, so its
 * content will be fully stored on a single member in the cluster and its
 * backup in another member in the cluster.
 */
public class RingbufferConfig implements SplitBrainMergeTypeProvider, IdentifiedDataSerializable, NamedConfig {

    /**
     * Default value of capacity of the RingBuffer.
     */
    public static final int DEFAULT_CAPACITY = 10 * 1000;
    /**
     * Default value of synchronous backup count
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    /**
     * Default value of asynchronous backup count
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;
    /**
     * Default value for the time to live property.
     */
    public static final int DEFAULT_TTL_SECONDS = 0;
    /**
     * Default value for the in-memory format.
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    private String name;
    private int capacity = DEFAULT_CAPACITY;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig().setEnabled(false);
    private String splitBrainProtectionName;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    public RingbufferConfig() {
    }

    /**
     * Creates a RingbufferConfig with the provided name.
     *
     * @param name the name
     * @throws java.lang.NullPointerException if name is {@code null}
     */
    public RingbufferConfig(String name) {
        this.name = checkNotNull(name, "name can't be null");
    }

    /**
     * Clones a RingbufferConfig
     *
     * @param config the ringbuffer config to clone
     * @throws java.lang.NullPointerException if config is {@code null}
     */
    public RingbufferConfig(RingbufferConfig config) {
        checkNotNull(config, "config can't be null");
        this.name = config.name;
        this.capacity = config.capacity;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
        this.inMemoryFormat = config.inMemoryFormat;
        if (config.ringbufferStoreConfig != null) {
            this.ringbufferStoreConfig = new RingbufferStoreConfig(config.ringbufferStoreConfig);
        }
        this.mergePolicyConfig = config.mergePolicyConfig;
        this.splitBrainProtectionName = config.splitBrainProtectionName;
    }

    /**
     * Creates a new RingbufferConfig by cloning an existing config and
     * overriding the name.
     *
     * @param name   the new name
     * @param config the config
     * @throws java.lang.NullPointerException if name or config is {@code null}
     */
    public RingbufferConfig(String name, RingbufferConfig config) {
        this(config);
        this.name = checkNotNull(name, "name can't be null");
    }

    /**
     * Sets the name of the ringbuffer.
     *
     * @param name the name of the ringbuffer
     * @return the updated {@link RingbufferConfig}
     * @throws IllegalArgumentException if name is {@code null} or an empty string
     */
    public RingbufferConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Returns the name of the ringbuffer.
     *
     * @return the name of the ringbuffer
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the capacity of the ringbuffer.
     * <p>
     * The capacity is the total number of items in the ringbuffer. The items
     * will remain in the ringbuffer, but the oldest items will eventually be
     * overwritten by the newest items.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of the ringbuffer.
     *
     * @param capacity the capacity
     * @return the updated Config
     * @throws java.lang.IllegalArgumentException if capacity smaller than 1
     * @see #getCapacity()
     */
    public RingbufferConfig setCapacity(int capacity) {
        this.capacity = checkPositive(capacity, "capacity can't be smaller than 1");
        return this;
    }

    /**
     * Gets the number of synchronous backups.
     *
     * @return number of synchronous backups
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     * @see #getBackupCount()
     */
    public RingbufferConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of asynchronous backups.
     *
     * @return the number of asynchronous backups
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public RingbufferConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups: backupCount plus asyncBackupCount.
     *
     * @return the total number of backups: backupCount plus asyncBackupCount
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Gets the time to live in seconds.
     *
     * @return the time to live in seconds or 0 if the items don't expire
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Sets the time to live in seconds which is the maximum number of seconds
     * for each item to stay in the ringbuffer before being removed.
     * <p>
     * Entries that are older than {@code timeToLiveSeconds} are removed from the
     * ringbuffer on the next ringbuffer operation (read or write).
     * <p>
     * Time to live can be disabled by setting {@code timeToLiveSeconds} to 0.
     * It means that items won't get removed because they expire. They may only
     * be overwritten.
     * When {@code timeToLiveSeconds} is disabled and after the tail does a full
     * loop in the ring, the ringbuffer size will always be equal to the capacity.
     * <p>
     * The {@code timeToLiveSeconds} can be any integer between 0 and
     * {@link Integer#MAX_VALUE}. 0 means infinite. The default is 0.
     *
     * @param timeToLiveSeconds the time to live period in seconds
     * @return the updated RingbufferConfig
     * @throws IllegalArgumentException if timeToLiveSeconds smaller than 0
     */
    public RingbufferConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = checkNotNegative(timeToLiveSeconds, "timeToLiveSeconds can't be smaller than 0");
        return this;
    }

    /**
     * Returns the in-memory format.
     * <p>
     * The in-memory format controls the format of the stored item in the
     * ringbuffer:
     * <ol>
     * <li>{@link InMemoryFormat#OBJECT}: the item is stored in deserialized
     * format (a regular object)</li>
     * <li>{@link InMemoryFormat#BINARY}: the item is stored in serialized format
     * (a binary blob) </li>
     * </ol>
     * <p>
     * The default is binary. The object InMemoryFormat is useful when:
     * <ol>
     * <li>the object stored in object format has a smaller footprint than in
     * binary format</li>
     * <li>if there are readers using a filter. Since for every filter
     * invocation, the object needs to be available in object format.</li>
     * </ol>
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets the in-memory format.
     * <p>
     * The in-memory format controls the format of the stored item in the
     * ringbuffer:
     * <ol>
     * <li>{@link InMemoryFormat#OBJECT}: the item is stored in deserialized
     * format (a regular object)</li>
     * <li>{@link InMemoryFormat#BINARY}: the item is stored in serialized format
     * (a binary blob) </li>
     * </ol>
     * <p>
     * The default is binary. The object InMemoryFormat is useful when:
     * <ol>
     * <li>the object stored in object format has a smaller footprint than in
     * binary format</li>
     * <li>if there are readers using a filter. Since for every filter
     * invocation, the object needs to be available in object format.</li>
     * </ol>
     *
     * @param inMemoryFormat the new in memory format
     * @return the updated Config
     * @throws NullPointerException     if inMemoryFormat is {@code null}
     * @throws IllegalArgumentException if {@link InMemoryFormat#NATIVE} in-memory
     *                                  format is selected
     */
    public RingbufferConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        checkNotNull(inMemoryFormat, "inMemoryFormat can't be null");
        checkFalse(inMemoryFormat == NATIVE, "InMemoryFormat " + NATIVE + " is not supported");
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    /**
     * Get the RingbufferStore (load and store ringbuffer items from/to a database)
     * configuration.
     *
     * @return the ringbuffer store configuration
     */
    public RingbufferStoreConfig getRingbufferStoreConfig() {
        return ringbufferStoreConfig;
    }

    /**
     * Set the RingbufferStore (load and store ringbuffer items from/to a database)
     * configuration.
     *
     * @param ringbufferStoreConfig set the RingbufferStore configuration to
     *                              this configuration
     * @return the ringbuffer configuration
     */
    public RingbufferConfig setRingbufferStoreConfig(RingbufferStoreConfig ringbufferStoreConfig) {
        this.ringbufferStoreConfig = ringbufferStoreConfig;
        return this;
    }

    /**
     * Returns the split brain protection name for operations.
     *
     * @return the split brain protection name
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated configuration
     */
    public RingbufferConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this ringbuffer.
     *
     * @return the {@link MergePolicyConfig} for this ringbuffer
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this ringbuffer.
     *
     * @return the ringbuffer configuration
     */
    public RingbufferConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = mergePolicyConfig;
        return this;
    }

    @Override
    public Class getProvidedMergeTypes() {
        return SplitBrainMergeTypes.RingbufferMergeTypes.class;
    }

    @Override
    public String toString() {
        return "RingbufferConfig{"
                + "name='" + name + '\''
                + ", capacity=" + capacity
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + ", inMemoryFormat=" + inMemoryFormat
                + ", ringbufferStoreConfig=" + ringbufferStoreConfig
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.RINGBUFFER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(capacity);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(timeToLiveSeconds);
        out.writeUTF(inMemoryFormat.name());
        out.writeObject(ringbufferStoreConfig);
        out.writeUTF(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        capacity = in.readInt();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        timeToLiveSeconds = in.readInt();
        inMemoryFormat = InMemoryFormat.valueOf(in.readUTF());
        ringbufferStoreConfig = in.readObject();
        splitBrainProtectionName = in.readUTF();
        mergePolicyConfig = in.readObject();
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RingbufferConfig)) {
            return false;
        }

        RingbufferConfig that = (RingbufferConfig) o;
        if (capacity != that.capacity) {
            return false;
        }
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (timeToLiveSeconds != that.timeToLiveSeconds) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (ringbufferStoreConfig != null ? !ringbufferStoreConfig.equals(that.ringbufferStoreConfig)
                : that.ringbufferStoreConfig != null) {
            return false;
        }
        if (splitBrainProtectionName != null ? !splitBrainProtectionName.equals(that.splitBrainProtectionName)
                : that.splitBrainProtectionName != null) {
            return false;
        }
        return mergePolicyConfig != null ? mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig == null;
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + capacity;
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + timeToLiveSeconds;
        result = 31 * result + (inMemoryFormat != null ? inMemoryFormat.hashCode() : 0);
        result = 31 * result + (ringbufferStoreConfig != null ? ringbufferStoreConfig.hashCode() : 0);
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public RingbufferConfig getAsReadOnly() {
        return new RingbufferConfigReadOnly(this);
    }

    /**
     * A readonly version of the {@link RingbufferConfig}.
     */
    private static class RingbufferConfigReadOnly extends RingbufferConfig {

        RingbufferConfigReadOnly(RingbufferConfig config) {
            super(config);
        }

        @Override
        public RingbufferStoreConfig getRingbufferStoreConfig() {
            final RingbufferStoreConfig storeConfig = super.getRingbufferStoreConfig();
            if (storeConfig != null) {
                return storeConfig.getAsReadOnly();
            } else {
                return null;
            }
        }

        @Override
        public RingbufferConfig setCapacity(int capacity) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setAsyncBackupCount(int asyncBackupCount) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setBackupCount(int backupCount) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setRingbufferStoreConfig(RingbufferStoreConfig ringbufferStoreConfig) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
            throw throwReadOnly();
        }

        @Override
        public RingbufferConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw throwReadOnly();
        }

        private UnsupportedOperationException throwReadOnly() {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
