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
/**
 * Configuration fot Multimap
 */
public class MultiMapConfig {

    /**
     * The number of default synchronous backup count
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    /**
     * The number of default asynchronous backup count
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private String valueCollectionType = ValueCollectionType.SET.toString();
    private List<EntryListenerConfig> listenerConfigs;
    private boolean binary = true;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private boolean statisticsEnabled = true;
    //    private PartitioningStrategyConfig partitionStrategyConfig;
    private MultiMapConfigReadOnly readOnly;

    public MultiMapConfig() {
    }

    public MultiMapConfig(MultiMapConfig defConfig) {
        this.name = defConfig.getName();
        this.valueCollectionType = defConfig.valueCollectionType;
        this.binary = defConfig.binary;
        this.backupCount = defConfig.backupCount;
        this.asyncBackupCount = defConfig.asyncBackupCount;
        this.statisticsEnabled = defConfig.statisticsEnabled;
        this.listenerConfigs = new ArrayList<EntryListenerConfig>(defConfig.getEntryListenerConfigs());
//        this.partitionStrategyConfig = defConfig.getPartitioningStrategyConfig();
    }

    public MultiMapConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MultiMapConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Type of value collection
     */
    public enum ValueCollectionType {
        /**
         * Store value collection as set
         */
        SET,
        /**
         * Store value collection as list
         */
        LIST
    }

    public String getName() {
        return name;
    }

    public MultiMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    public ValueCollectionType getValueCollectionType() {
        return ValueCollectionType.valueOf(valueCollectionType.toUpperCase());
    }

    public MultiMapConfig setValueCollectionType(String valueCollectionType) {
        this.valueCollectionType = valueCollectionType;
        return this;
    }

    public MultiMapConfig setValueCollectionType(ValueCollectionType valueCollectionType) {
        this.valueCollectionType = valueCollectionType.toString();
        return this;
    }

    public MultiMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<EntryListenerConfig>();
        }
        return listenerConfigs;
    }

    public MultiMapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public boolean isBinary() {
        return binary;
    }

    public MultiMapConfig setBinary(boolean binary) {
        this.binary = binary;
        return this;
    }

    @Deprecated
    public int getSyncBackupCount() {
        return backupCount;
    }

    @Deprecated
    public MultiMapConfig setSyncBackupCount(int syncBackupCount) {
        this.backupCount = syncBackupCount;
        return this;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public MultiMapConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public MultiMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

//    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
//        return partitionStrategyConfig;
//    }
//
//    public MultiMapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitionStrategyConfig) {
//        this.partitionStrategyConfig = partitionStrategyConfig;
//        return this;
//    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MultiMapConfig");
        sb.append("{name='").append(name).append('\'');
        sb.append(", valueCollectionType='").append(valueCollectionType).append('\'');
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", binary=").append(binary);
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        return sb.toString();
    }
}
