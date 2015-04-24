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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IQueue}
 */
public class QueueConfig {

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
    private QueueConfigReadOnly readOnly;

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
        this.queueStoreConfig = config.queueStoreConfig != null ? new QueueStoreConfig(config.queueStoreConfig) : null;
        this.listenerConfigs = new ArrayList<ItemListenerConfig>(config.getItemListenerConfigs());
    }

    /**
     * Returns a read only copy of the queue configuration.
     *
     * @return A read only copy of the queue configuration.
     */
    public QueueConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new QueueConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the TTL (time to live) for empty Queue.
     *
     * @return The TTL (time to live) for empty Queue.
     */
    public int getEmptyQueueTtl() {
        return emptyQueueTtl;
    }

    /**
     * Sets the TTL (time to live) for empty Queue.
     *
     * @param emptyQueueTtl Set the TTL (time to live) for empty Queue to this value.
     * @return The Queue configuration.
     */
    public QueueConfig setEmptyQueueTtl(int emptyQueueTtl) {
        this.emptyQueueTtl = emptyQueueTtl;
        return this;
    }

    /**
     * Returns the maximum size of the Queue.
     *
     * @return The maximum size of the Queue.
     */
    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    /**
     * Sets the maximum size of the Queue.
     *
     * @param maxSize Set the maximum size of the Queue to this value.
     * @return The Queue configuration.
     */
    public QueueConfig setMaxSize(int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("Size of the queue can not be a negative value!");
        }
        this.maxSize = maxSize;
        return this;
    }

    /**
     * Get the total of the backup count and the asynchronous backup count.
     *
     * @return The total of the backup count and the asynchronous backup count.
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Get the backup count.
     *
     * @return The backup count.
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the current QueueConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public QueueConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Get the asynchronous backup count.
     *
     * @return The asynchronous backup count.
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated QueueConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public QueueConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Get the queue store configuration.
     *
     * @return The queue store configuration.
     */
    public QueueStoreConfig getQueueStoreConfig() {
        return queueStoreConfig;
    }

    /**
     * Set the queue store configuration.
     *
     * @param queueStoreConfig Set the queue store configuration to this configuration.
     * @return The queue store configuration.
     */
    public QueueConfig setQueueStoreConfig(QueueStoreConfig queueStoreConfig) {
        this.queueStoreConfig = queueStoreConfig;
        return this;
    }

    /**
     * Check if statistics are enabled.
     *
     * @return true is statistics are enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Set the statistics enabled value for this queue configuration.
     *
     * @param statisticsEnabled Set to true or false to enable or disable statistics.
     * @return The queue configuration.
     */
    public QueueConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * @return The name of this queue configuration.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name for this queue configuration.
     *
     * @param name The name to set for this queue configuration.
     * @return This queue configuration.
     */
    public QueueConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Add an item listener configuration to this queue configuration.
     *
     * @param listenerConfig The item listener configuration to add to this queue configuration.
     * @return This queue configuration.
     */
    public QueueConfig addItemListenerConfig(ItemListenerConfig listenerConfig) {
        getItemListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Get the list of item listener configurations for this queue configuration.
     *
     * @return The list of item listener configurations for this queue configuration.
     */
    public List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

    /**
     * Set the list of item listener configurations for this queue configuration to this list.
     *
     * @param listenerConfigs The list of item listener configurations to set for this queue configuration.
     * @return This queue configuration.
     */
    public QueueConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueueConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", emptyQueueTtl=").append(emptyQueueTtl);
        sb.append(", queueStoreConfig=").append(queueStoreConfig);
        sb.append(", statisticsEnabled=").append(statisticsEnabled);
        sb.append('}');
        return sb.toString();
    }
}
