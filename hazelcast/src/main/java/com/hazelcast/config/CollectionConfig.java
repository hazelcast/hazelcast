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
 * Provides configuration service for Collection.
 *
 * @param <T> Type of Collection such as List, Set
 */
public abstract class CollectionConfig<T extends CollectionConfig> {

    /**
     * Maximum size Configuration
     */
    public static final int DEFAULT_MAX_SIZE = 0;
    /**
     * Synchronous Backup Counter
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    /**
     * Asynchronous Backup Counter
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private List<ItemListenerConfig> listenerConfigs;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    private boolean statisticsEnabled = true;

    protected CollectionConfig() {
    }

    protected CollectionConfig(CollectionConfig config) {
        this.name = config.name;
        this.listenerConfigs = new ArrayList<ItemListenerConfig>(config.getItemListenerConfigs());
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.maxSize = config.maxSize;
        this.statisticsEnabled = config.statisticsEnabled;
    }

    public abstract T getAsReadOnly();

    public String getName() {
        return name;
    }

    public T setName(String name) {
        this.name = name;
        return (T) this;
    }

    public List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

    public T setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return (T) this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the current CollectionConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public T setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return (T) this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated CollectionConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public T setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(asyncBackupCount, asyncBackupCount);
        return (T) this;
    }

    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    public T setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return (T) this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public T setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return (T) this;
    }

    public void addItemListenerConfig(ItemListenerConfig itemListenerConfig) {
        getItemListenerConfigs().add(itemListenerConfig);
    }
}
