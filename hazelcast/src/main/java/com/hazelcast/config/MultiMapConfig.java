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
import java.util.Collections;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;

/**
 * Configuration for Multi-map.
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

    /**
     * Default value collection type of multi-map.
     */
    public static final ValueCollectionType DEFAULT_VALUE_COLLECTION_TYPE = ValueCollectionType.SET;

    private String name;
    private String valueCollectionType = DEFAULT_VALUE_COLLECTION_TYPE.toString();
    private List<EntryListenerConfig> listenerConfigs;
    private boolean binary = true;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private boolean statisticsEnabled = true;
    //    private PartitioningStrategyConfig partitionStrategyConfig;
    private MultiMapConfigReadOnly readOnly;

    public MultiMapConfig() {
    }

    public MultiMapConfig(String name) {
        setName(name);
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

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the current MultiMapConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public MultiMapConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated MultiMapConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
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

    /**
     * Contains the configuration for an {@link com.hazelcast.core.MultiMap}.
     */
    static class MultiMapConfigReadOnly extends MultiMapConfig {

        public MultiMapConfigReadOnly(MultiMapConfig defConfig) {
            super(defConfig);
        }

        public List<EntryListenerConfig> getEntryListenerConfigs() {
            final List<EntryListenerConfig> listenerConfigs = super.getEntryListenerConfigs();
            final List<EntryListenerConfig> readOnlyListenerConfigs = new ArrayList<EntryListenerConfig>(listenerConfigs.size());
            for (EntryListenerConfig listenerConfig : listenerConfigs) {
                readOnlyListenerConfigs.add(listenerConfig.getAsReadOnly());
            }
            return Collections.unmodifiableList(readOnlyListenerConfigs);
        }

        public MultiMapConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setValueCollectionType(String valueCollectionType) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setValueCollectionType(ValueCollectionType valueCollectionType) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setBinary(boolean binary) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setSyncBackupCount(int syncBackupCount) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setBackupCount(int backupCount) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }

        public MultiMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
            throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
        }
    }
}
