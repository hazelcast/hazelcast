/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IQueue}.
 */
public class QueueConfig implements IdentifiedDataSerializable, Versioned {

    /**
     * Default value for the maximum size of the Queue.
     */
    public static final int DEFAULT_MAX_SIZE = 0;

    /**
     * Default value for the sychronous backup count.
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * Default value of the asynchronous backup count.
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    /**
     * Default value for the TTL (time to live) for empty Queue.
     */
    public static final int DEFAULT_EMPTY_QUEUE_TTL = -1;

    private String name;
    private List<ItemListenerConfig> listenerConfigs;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    private int emptyQueueTtl = DEFAULT_EMPTY_QUEUE_TTL;
    private QueueStoreConfig queueStoreConfig;
    private boolean statisticsEnabled = true;
    private String quorumName;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
    private transient QueueConfigReadOnly readOnly;

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
        this.quorumName = config.quorumName;
        this.mergePolicyConfig = config.mergePolicyConfig;
        this.queueStoreConfig = config.queueStoreConfig != null ? new QueueStoreConfig(config.queueStoreConfig) : null;
        this.listenerConfigs = new ArrayList<ItemListenerConfig>(config.getItemListenerConfigs());
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public QueueConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new QueueConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the TTL (time to live) for emptying the Queue.
     *
     * @return the TTL (time to live) for emptying the Queue
     */
    public int getEmptyQueueTtl() {
        return emptyQueueTtl;
    }

    /**
     * Sets the TTL (time to live) for emptying the Queue.
     *
     * @param emptyQueueTtl set the TTL (time to live) for emptying the Queue to this value
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
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
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
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
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
    public QueueStoreConfig getQueueStoreConfig() {
        return queueStoreConfig;
    }

    /**
     * Set the QueueStore (load and store queue items from/to a database) configuration.
     *
     * @param queueStoreConfig set the QueueStore configuration to this configuration
     * @return the QueueStore configuration
     */
    public QueueConfig setQueueStoreConfig(QueueStoreConfig queueStoreConfig) {
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
    public String getName() {
        return name;
    }

    /**
     * Set the name for this queue.
     *
     * @param name the name to set for this queue
     * @return this queue configuration
     */
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
    public List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

    /**
     * Set the list of item listener configurations for this queue.
     *
     * @param listenerConfigs the list of item listener configurations to set for this queue
     * @return the updated queue configuration
     */
    public QueueConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    /**
     * Returns the quorum name for queue operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for queue operations.
     *
     * @param quorumName the quorum name
     * @return the updated queue configuration
     */
    public QueueConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this queue.
     *
     * @return the {@link MergePolicyConfig} for this queue
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this queue.
     *
     * @return the updated queue configuration
     */
    public QueueConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
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
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.QUEUE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        writeNullableList(listenerConfigs, out);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(maxSize);
        out.writeInt(emptyQueueTtl);
        out.writeObject(queueStoreConfig);
        out.writeBoolean(statisticsEnabled);
        out.writeUTF(quorumName);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeObject(mergePolicyConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        listenerConfigs = readNullableList(in);
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        maxSize = in.readInt();
        emptyQueueTtl = in.readInt();
        queueStoreConfig = in.readObject();
        statisticsEnabled = in.readBoolean();
        quorumName = in.readUTF();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            mergePolicyConfig = in.readObject();
        }
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueConfig)) {
            return false;
        }

        QueueConfig that = (QueueConfig) o;
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (getMaxSize() != that.getMaxSize()) {
            return false;
        }
        if (emptyQueueTtl != that.emptyQueueTtl) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!getItemListenerConfigs().equals(that.getItemListenerConfigs())) {
            return false;
        }
        if (queueStoreConfig != null ? !queueStoreConfig.equals(that.queueStoreConfig) : that.queueStoreConfig != null) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        return mergePolicyConfig != null ? mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig == null;
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + getItemListenerConfigs().hashCode();
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + getMaxSize();
        result = 31 * result + emptyQueueTtl;
        result = 31 * result + (queueStoreConfig != null ? queueStoreConfig.hashCode() : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }
}
