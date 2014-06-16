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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

public class QueueConfig {

    public static final int DEFAULT_MAX_SIZE = 0;
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;
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

    public QueueConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new QueueConfigReadOnly(this);
        }
        return readOnly;
    }

    public int getEmptyQueueTtl() {
        return emptyQueueTtl;
    }

    public QueueConfig setEmptyQueueTtl(int emptyQueueTtl) {
        this.emptyQueueTtl = emptyQueueTtl;
        return this;
    }

    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    public QueueConfig setMaxSize(int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("Size of the queue can not be a negative value!");
        }
        this.maxSize = maxSize;
        return this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public QueueConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public QueueConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public QueueStoreConfig getQueueStoreConfig() {
        return queueStoreConfig;
    }

    public QueueConfig setQueueStoreConfig(QueueStoreConfig queueStoreConfig) {
        this.queueStoreConfig = queueStoreConfig;
        return this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public QueueConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     * @return this queue config
     */
    public QueueConfig setName(String name) {
        this.name = name;
        return this;
    }

    public QueueConfig addItemListenerConfig(ItemListenerConfig listenerConfig) {
        getItemListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

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
