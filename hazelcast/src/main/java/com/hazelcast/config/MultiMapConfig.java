/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for MultiMap.
 */
@SuppressWarnings("checkstyle:methodcount")
public class MultiMapConfig implements IdentifiedDataSerializable, NamedConfig {

    /**
     * The default number of synchronous backups for this MultiMap.
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * The default number of asynchronous backups for this MultiMap.
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    /**
     * Default value collection type of this MultiMap.
     */
    public static final ValueCollectionType DEFAULT_VALUE_COLLECTION_TYPE = ValueCollectionType.SET;

    private String name;
    private String valueCollectionType = DEFAULT_VALUE_COLLECTION_TYPE.toString();
    private List<EntryListenerConfig> listenerConfigs = new ArrayList<EntryListenerConfig>();
    private boolean binary = true;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private boolean statisticsEnabled = true;
    private String splitBrainProtectionName;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    public MultiMapConfig() {
    }

    public MultiMapConfig(String name) {
        setName(name);
    }

    public MultiMapConfig(MultiMapConfig config) {
        this.name = config.getName();
        this.valueCollectionType = config.valueCollectionType;
        this.listenerConfigs.addAll(config.listenerConfigs);
        this.binary = config.binary;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.statisticsEnabled = config.statisticsEnabled;
        this.splitBrainProtectionName = config.splitBrainProtectionName;
        this.mergePolicyConfig = new MergePolicyConfig(config.mergePolicyConfig);
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

    /**
     * Gets the name of this MultiMap.
     *
     * @return the name of this MultiMap
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this MultiMap.
     *
     * @param name the name to set for this MultiMap
     * @return this updated MultiMap configuration
     */
    public MultiMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the collection type for the values of this MultiMap.
     *
     * @return the collection type for the values of this MultiMap
     */
    public ValueCollectionType getValueCollectionType() {
        return ValueCollectionType.valueOf(valueCollectionType.toUpperCase(StringUtil.LOCALE_INTERNAL));
    }

    /**
     * Sets the collection type for the values of this MultiMap.
     *
     * @param valueCollectionType the collection type for the values of this MultiMap (SET or LIST)
     * @return this updated MultiMap configuration
     */
    public MultiMapConfig setValueCollectionType(String valueCollectionType) {
        this.valueCollectionType = valueCollectionType;
        return this;
    }

    /**
     * Sets the collection type for the values of this MultiMap.
     *
     * @param valueCollectionType the collection type for the values of this MultiMap (SET or LIST)
     * @return this updated MultiMap configuration
     */
    public MultiMapConfig setValueCollectionType(ValueCollectionType valueCollectionType) {
        this.valueCollectionType = valueCollectionType.toString();
        return this;
    }

    /**
     * Adds an entry listener to this MultiMap (listens for when entries are added or removed).
     *
     * @param listenerConfig the entry listener to add to this MultiMap
     */
    public MultiMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Gets the list of entry listeners (listens for when entries are added or removed) for this MultiMap.
     *
     * @return the list of entry listeners for this MultiMap
     */
    public List<EntryListenerConfig> getEntryListenerConfigs() {
        return listenerConfigs;
    }

    /**
     * Sets the list of entry listeners (listens for when entries are added or removed) for this MultiMap.
     *
     * @param listenerConfigs the list of entry listeners for this MultiMap
     * @return this updated MultiMap configuration
     */
    public MultiMapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    /**
     * Checks if the MultiMap is in binary (serialized) form.
     *
     * @return {@code true} if the MultiMap is in binary (serialized) form, {@code false} otherwise
     */
    public boolean isBinary() {
        return binary;
    }

    /**
     * Enables or disables binary (serialized) form for this MultiMap.
     *
     * @param binary {@code true} to set the MultiMap to binary (serialized) form, {@code false} otherwise
     * @return this updated MultiMap configuration
     */

    public MultiMapConfig setBinary(boolean binary) {
        this.binary = binary;
        return this;
    }

    /**
     * Gets the number of synchronous backups for this MultiMap.
     *
     * @return the number of synchronous backups for this MultiMap
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set for this MultiMap
     * @return the current MultiMapConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public MultiMapConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of asynchronous backups for this MultiMap.
     *
     * @return the number of asynchronous backups for this MultiMap
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated MultiMapConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the total number of backups (synchronous + asynchronous) for this MultiMap.
     *
     * @return the total number of backups (synchronous + asynchronous) for this MultiMap
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Checks to see if statistics are enabled for this MultiMap.
     *
     * @return {@code true} if statistics are enabled for this MultiMap, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this MultiMap.
     *
     * @param statisticsEnabled {@code true} to enable statistics for this MultiMap, {@code false} to disable
     * @return the updated MultiMapConfig
     */
    public MultiMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
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
    public MultiMapConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this MultiMap.
     *
     * @return the {@link MergePolicyConfig} for this MultiMap
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this MultiMap.
     *
     * @return the updated MultiMapConfig
     */
    public MultiMapConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        return this;
    }

    public String toString() {
        return "MultiMapConfig{"
                + "name='" + name + '\''
                + ", valueCollectionType='" + valueCollectionType + '\''
                + ", listenerConfigs=" + listenerConfigs
                + ", binary=" + binary
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
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
        return ConfigDataSerializerHook.MULTIMAP_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(valueCollectionType);
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(listenerConfigs.size());
            for (ListenerConfig listenerConfig : listenerConfigs) {
                out.writeObject(listenerConfig);
            }
        }
        out.writeBoolean(binary);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeBoolean(statisticsEnabled);
        out.writeString(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        valueCollectionType = in.readString();
        boolean hasListenerConfig = in.readBoolean();
        if (hasListenerConfig) {
            int configSize = in.readInt();
            listenerConfigs = new ArrayList<EntryListenerConfig>(configSize);
            for (int i = 0; i < configSize; i++) {
                EntryListenerConfig listenerConfig = in.readObject();
                listenerConfigs.add(listenerConfig);
            }
        }
        binary = in.readBoolean();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        statisticsEnabled = in.readBoolean();
        splitBrainProtectionName = in.readString();
        mergePolicyConfig = in.readObject();
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MultiMapConfig)) {
            return false;
        }

        MultiMapConfig that = (MultiMapConfig) o;
        if (binary != that.binary) {
            return false;
        }
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (valueCollectionType != null
                ? !valueCollectionType.equals(that.valueCollectionType)
                : that.valueCollectionType != null) {
            return false;
        }
        if (listenerConfigs != null ? !listenerConfigs.equals(that.listenerConfigs) : that.listenerConfigs != null) {
            return false;
        }
        if (splitBrainProtectionName != null ? !splitBrainProtectionName.equals(that.splitBrainProtectionName)
                : that.splitBrainProtectionName != null) {
            return false;
        }
        return mergePolicyConfig != null ? mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig == null;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (valueCollectionType != null ? valueCollectionType.hashCode() : 0);
        result = 31 * result + (listenerConfigs != null ? listenerConfigs.hashCode() : 0);
        result = 31 * result + (binary ? 1 : 0);
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }
}
