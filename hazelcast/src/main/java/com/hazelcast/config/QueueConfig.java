/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.IQueue;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.cluster.Versions.V5_4;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for an {@link IQueue}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class QueueConfig implements IdentifiedDataSerializable, NamedConfig, Versioned,
                                    UserCodeNamespaceAwareConfig<QueueConfig> {

    /**
     * Default value for the maximum size of the Queue.
     */
    public static final int DEFAULT_MAX_SIZE = 0;

    /**
     * Default value for the synchronous backup count.
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * Default value of the asynchronous backup count.
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    /**
     * Default value for the {@link #getEmptyQueueTtl()}
     */
    public static final int DEFAULT_EMPTY_QUEUE_TTL = -1;

    private String name;
    private List<ItemListenerConfig> listenerConfigs;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    /** @see #getEmptyQueueTtl() */
    private int emptyQueueTtl = DEFAULT_EMPTY_QUEUE_TTL;
    private QueueStoreConfig queueStoreConfig;
    private boolean statisticsEnabled = true;
    private String splitBrainProtectionName;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
    private String priorityComparatorClassName;
    private @Nullable String userCodeNamespace = DEFAULT_NAMESPACE;

    public QueueConfig() {
    }

    public QueueConfig(String name) {
        setName(name);
    }

    public QueueConfig(QueueConfig config) {
        this();
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.maxSize = config.maxSize;
        this.emptyQueueTtl = config.emptyQueueTtl;
        this.statisticsEnabled = config.statisticsEnabled;
        this.splitBrainProtectionName = config.splitBrainProtectionName;
        this.mergePolicyConfig = new MergePolicyConfig(config.mergePolicyConfig);
        this.queueStoreConfig = config.queueStoreConfig != null ? new QueueStoreConfig(config.queueStoreConfig) : null;
        this.listenerConfigs = new ArrayList<>(config.getItemListenerConfigs());
        this.priorityComparatorClassName = config.priorityComparatorClassName;
        this.userCodeNamespace = config.userCodeNamespace;
    }

    /**
     * @return the TTL (time to live) for destroying the Queue (in seconds). <br>
     *         Once items have been removed and the Queue has been empty for a given amount of time, the Queue will be
     *         destroyed. Has no effect until the Queue has been populated.
     */
    public int getEmptyQueueTtl() {
        return emptyQueueTtl;
    }

    /**
     * @param emptyQueueTtl set the TTL (time to live) for destroying the Queue (in seconds). <br>
     *        Once items have been removed and the Queue has been empty for a given amount of time, the Queue will be destroyed.
     *        Has no effect until the Queue has been populated.
     * @return the Queue configuration
     */
    public QueueConfig setEmptyQueueTtl(int emptyQueueTtl) {
        this.emptyQueueTtl = emptyQueueTtl;
        return this;
    }

    /**
     * Returns the maximum size of the Queue.
     *
     * @return the maximum size of the Queue
     */
    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    /**
     * Sets the maximum size of the Queue.
     *
     * @param maxSize set the maximum size of the Queue to this value
     * @return the Queue configuration
     * @throws IllegalArgumentException if the provided max size is negative
     */
    public QueueConfig setMaxSize(int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("Size of the queue can not be a negative value!");
        }
        this.maxSize = maxSize;
        return this;
    }

    /**
     * Get the total number of backups: the backup count plus the asynchronous backup count.
     *
     * @return the total number of backups
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Get the number of synchronous backups for this queue.
     *
     * @return the synchronous backup count
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups for this queue.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the current QueueConfig
     * @throws IllegalArgumentException if backupCount is smaller than 0,
     *                                  or larger than the maximum number of backups,
     *                                  or the sum of the backups and async backups is larger than the maximum
     *                                  number of backups
     * @see #setAsyncBackupCount(int)
     */
    public QueueConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Get the number of asynchronous backups for this queue.
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
     * @return the updated QueueConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum
     *                                  number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public QueueConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Get the QueueStore (load and store queue items from/to a database) configuration.
     *
     * @return the QueueStore configuration
     */
    public @Nullable QueueStoreConfig getQueueStoreConfig() {
        return queueStoreConfig;
    }

    /**
     * Set the QueueStore (load and store queue items from/to a database) configuration.
     *
     * @param queueStoreConfig set the QueueStore configuration to this configuration
     * @return the QueueStore configuration
     */
    public QueueConfig setQueueStoreConfig(@Nullable QueueStoreConfig queueStoreConfig) {
        this.queueStoreConfig = queueStoreConfig;
        return this;
    }

    /**
     * Check if statistics are enabled for this queue.
     *
     * @return {@code true} if statistics are enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this queue.
     *
     * @param statisticsEnabled {@code true} to enable statistics for this queue, {@code false} to disable
     * @return the updated QueueConfig
     */
    public QueueConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * @return the name of this queue
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Set the name for this queue.
     *
     * @param name the name to set for this queue
     * @return this queue configuration
     */
    @Override
    public QueueConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Add an item listener configuration to this queue.
     *
     * @param listenerConfig the item listener configuration to add to this queue
     * @return the updated queue configuration
     */
    public QueueConfig addItemListenerConfig(ItemListenerConfig listenerConfig) {
        getItemListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Get the list of item listener configurations for this queue.
     *
     * @return the list of item listener configurations for this queue
     */
    public @Nonnull List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<>();
        }
        return listenerConfigs;
    }

    /**
     * Set the list of item listener configurations for this queue.
     *
     * @param listenerConfigs the list of item listener configurations to set for this queue
     * @return the updated queue configuration
     */
    public QueueConfig setItemListenerConfigs(@Nullable List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    /**
     * Returns the split brain protection name for queue operations.
     *
     * @return the split brain protection name
     */
    public @Nullable String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for queue operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated queue configuration
     */
    public QueueConfig setSplitBrainProtectionName(@Nullable String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this queue.
     *
     * @return the {@link MergePolicyConfig} for this queue
     */
    public @Nonnull MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this queue.
     *
     * @return the updated queue configuration
     */
    public QueueConfig setMergePolicyConfig(@Nonnull MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        return this;
    }

    /**
     * Check if underlying implementation is a {@code PriorityQueue}. Otherwise,
     * it is a FIFO queue.
     *
     * @return {@code true} if priority queue has been configured, {@code false}
     * otherwise
     */
    public boolean isPriorityQueue() {
        return !StringUtil.isNullOrEmptyAfterTrim(priorityComparatorClassName);
    }

    /**
     * Returns the class name that will be used to compare queue items.
     * If the returned class name is non-empty, the queue will behave as a priority
     * queue, otherwise it behaves as a FIFO queue.
     * <p>
     * If this value is non-null, then Hazelcast will ignore the queue store
     * {@link QueueStoreConfig#STORE_MEMORY_LIMIT} configuration value.
     */
    public @Nullable String getPriorityComparatorClassName() {
        return priorityComparatorClassName;
    }

    /**
     * Sets the class name that will be used to compare queue items.
     * If the provided class name is non-empty, the queue will behave as a priority
     * queue, otherwise it behaves as a FIFO queue.
     * <p>
     * Setting the comparator to a non-null value also makes the queue store ignore
     * the {@link QueueStoreConfig#STORE_MEMORY_LIMIT} configuration value.
     *
     * @param priorityComparatorClassName the class name that will be used to compare queue items
     * @return this QueueConfig instance
     */
    public QueueConfig setPriorityComparatorClassName(@Nullable String priorityComparatorClassName) {
        this.priorityComparatorClassName = priorityComparatorClassName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    /**
     * Associates the provided Namespace Name with this structure for {@link ClassLoader} awareness.
     * <p>
     * The behaviour of setting this to {@code null} is outlined in the documentation for
     * {@link UserCodeNamespaceAwareConfig#DEFAULT_NAMESPACE}.
     *
     * @param userCodeNamespace The ID of the Namespace to associate with this structure.
     * @return the updated {@link QueueConfig} instance
     * @since 5.4
     */
    @Override
    public QueueConfig setUserCodeNamespace(@Nullable String userCodeNamespace) {
        this.userCodeNamespace = userCodeNamespace;
        return this;
    }

    @Override
    public String toString() {
        return "QueueConfig{"
                + "name='" + name + '\''
                + ", listenerConfigs=" + listenerConfigs
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + ", maxSize=" + maxSize
                + ", emptyQueueTtl=" + emptyQueueTtl
                + ", queueStoreConfig=" + queueStoreConfig
                + ", statisticsEnabled=" + statisticsEnabled
                + ", mergePolicyConfig=" + mergePolicyConfig
                + ", priorityComparatorClassName=" + priorityComparatorClassName
                + ", userCodeNamespace=" + userCodeNamespace
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.QUEUE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        writeNullableList(listenerConfigs, out);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(maxSize);
        out.writeInt(emptyQueueTtl);
        out.writeObject(queueStoreConfig);
        out.writeBoolean(statisticsEnabled);
        out.writeString(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
        out.writeString(priorityComparatorClassName);

        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(V5_4)) {
            out.writeString(userCodeNamespace);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        listenerConfigs = readNullableList(in);
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        maxSize = in.readInt();
        emptyQueueTtl = in.readInt();
        queueStoreConfig = in.readObject();
        statisticsEnabled = in.readBoolean();
        splitBrainProtectionName = in.readString();
        mergePolicyConfig = in.readObject();
        priorityComparatorClassName = in.readString();

        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(V5_4)) {
            userCodeNamespace = in.readString();
        }
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueConfig that)) {
            return false;
        }
        return backupCount == that.backupCount
                && asyncBackupCount == that.asyncBackupCount
                && getMaxSize() == that.getMaxSize()
                && emptyQueueTtl == that.emptyQueueTtl
                && statisticsEnabled == that.statisticsEnabled
                && Objects.equals(name, that.name)
                && getItemListenerConfigs().equals(that.getItemListenerConfigs())
                && Objects.equals(queueStoreConfig, that.queueStoreConfig)
                && Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)
                && Objects.equals(mergePolicyConfig, that.mergePolicyConfig)
                && Objects.equals(priorityComparatorClassName, that.priorityComparatorClassName)
                && Objects.equals(userCodeNamespace, that.userCodeNamespace);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, getItemListenerConfigs(), backupCount, asyncBackupCount, getMaxSize(), emptyQueueTtl,
                queueStoreConfig, statisticsEnabled, splitBrainProtectionName, mergePolicyConfig,
                priorityComparatorClassName, userCodeNamespace);
    }
}
