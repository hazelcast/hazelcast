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

public abstract class CollectionConfig<T extends CollectionConfig> {

    public static final int DEFAULT_MAX_SIZE = 0;
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
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

    public T setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return (T) this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public T setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
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
