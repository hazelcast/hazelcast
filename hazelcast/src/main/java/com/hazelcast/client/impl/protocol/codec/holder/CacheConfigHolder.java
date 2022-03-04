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

package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CacheConfigHolder {
    private final String name;
    private final String managerPrefix;
    private final String uriString;
    private final int backupCount;
    private final int asyncBackupCount;
    private final String inMemoryFormat;
    private final EvictionConfigHolder evictionConfigHolder;
    private final WanReplicationRef wanReplicationRef;
    private final String keyClassName;
    private final String valueClassName;
    private final Data cacheLoaderFactory;
    private final Data cacheWriterFactory;
    private final Data expiryPolicyFactory;
    private final boolean isReadThrough;
    private final boolean isWriteThrough;
    private final boolean isStoreByValue;
    private final boolean isManagementEnabled;
    private final boolean isStatisticsEnabled;
    private final HotRestartConfig hotRestartConfig;
    private final boolean merkleTreeConfigExists;
    private final MerkleTreeConfig merkleTreeConfig;
    private final EventJournalConfig eventJournalConfig;
    private final String splitBrainProtectionName;
    private final List<Data> listenerConfigurations;
    private final MergePolicyConfig mergePolicyConfig;
    private final boolean disablePerEntryInvalidationEvents;
    private final List<ListenerConfigHolder> cachePartitionLostListenerConfigs;

    public CacheConfigHolder(String name, String managerPrefix, String uriString, int backupCount, int asyncBackupCount,
                             String inMemoryFormat, EvictionConfigHolder evictionConfigHolder,
                             WanReplicationRef wanReplicationRef, String keyClassName, String valueClassName,
                             Data cacheLoaderFactory, Data cacheWriterFactory, Data expiryPolicyFactory, boolean isReadThrough,
                             boolean isWriteThrough, boolean isStoreByValue, boolean isManagementEnabled,
                             boolean isStatisticsEnabled, HotRestartConfig hotRestartConfig,
                             EventJournalConfig eventJournalConfig, String splitBrainProtectionName,
                             List<Data> listenerConfigurations, MergePolicyConfig mergePolicyConfig,
                             boolean disablePerEntryInvalidationEvents,
                             List<ListenerConfigHolder> cachePartitionLostListenerConfigs, boolean merkleTreeConfigExists,
                             MerkleTreeConfig merkleTreeConfig) {
        this.name = name;
        this.managerPrefix = managerPrefix;
        this.uriString = uriString;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.inMemoryFormat = inMemoryFormat;
        this.evictionConfigHolder = evictionConfigHolder;
        this.wanReplicationRef = wanReplicationRef;
        this.keyClassName = keyClassName;
        this.valueClassName = valueClassName;
        this.cacheLoaderFactory = cacheLoaderFactory;
        this.cacheWriterFactory = cacheWriterFactory;
        this.expiryPolicyFactory = expiryPolicyFactory;
        this.isReadThrough = isReadThrough;
        this.isWriteThrough = isWriteThrough;
        this.isStoreByValue = isStoreByValue;
        this.isManagementEnabled = isManagementEnabled;
        this.isStatisticsEnabled = isStatisticsEnabled;
        this.hotRestartConfig = hotRestartConfig;
        this.eventJournalConfig = eventJournalConfig;
        this.splitBrainProtectionName = splitBrainProtectionName;
        this.listenerConfigurations = listenerConfigurations;
        this.mergePolicyConfig = mergePolicyConfig;
        this.disablePerEntryInvalidationEvents = disablePerEntryInvalidationEvents;
        this.cachePartitionLostListenerConfigs = cachePartitionLostListenerConfigs;
        this.merkleTreeConfigExists = merkleTreeConfigExists;
        this.merkleTreeConfig = merkleTreeConfig;
    }

    public String getName() {
        return name;
    }

    public String getManagerPrefix() {
        return managerPrefix;
    }

    public String getUriString() {
        return uriString;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public String getInMemoryFormat() {
        return inMemoryFormat;
    }

    public EvictionConfigHolder getEvictionConfigHolder() {
        return evictionConfigHolder;
    }

    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    public String getKeyClassName() {
        return keyClassName;
    }

    public String getValueClassName() {
        return valueClassName;
    }

    public Data getCacheLoaderFactory() {
        return cacheLoaderFactory;
    }

    public Data getCacheWriterFactory() {
        return cacheWriterFactory;
    }

    public Data getExpiryPolicyFactory() {
        return expiryPolicyFactory;
    }

    public boolean isReadThrough() {
        return isReadThrough;
    }

    public boolean isWriteThrough() {
        return isWriteThrough;
    }

    public boolean isStoreByValue() {
        return isStoreByValue;
    }

    public boolean isManagementEnabled() {
        return isManagementEnabled;
    }

    public boolean isStatisticsEnabled() {
        return isStatisticsEnabled;
    }

    public HotRestartConfig getHotRestartConfig() {
        return hotRestartConfig;
    }

    public EventJournalConfig getEventJournalConfig() {
        return eventJournalConfig;
    }

    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    public List<Data> getListenerConfigurations() {
        return listenerConfigurations;
    }

    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    public boolean isDisablePerEntryInvalidationEvents() {
        return disablePerEntryInvalidationEvents;
    }

    public List<ListenerConfigHolder> getCachePartitionLostListenerConfigs() {
        return cachePartitionLostListenerConfigs;
    }

    public MerkleTreeConfig getMerkleTreeConfig() {
        return merkleTreeConfig;
    }

    public boolean isMerkleTreeConfigExists() {
        return merkleTreeConfigExists;
    }

    public <K, V> CacheConfig<K, V> asCacheConfig(SerializationService serializationService) {
        CacheConfig<K, V> config = new CacheConfig();
        config.setName(name);
        config.setManagerPrefix(managerPrefix);
        config.setUriString(uriString);
        config.setBackupCount(backupCount);
        config.setAsyncBackupCount(asyncBackupCount);
        config.setInMemoryFormat(InMemoryFormat.valueOf(inMemoryFormat));
        config.setEvictionConfig(evictionConfigHolder.asEvictionConfig(serializationService));
        config.setWanReplicationRef(wanReplicationRef);
        config.setKeyClassName(keyClassName);
        config.setValueClassName(valueClassName);
        config.setCacheLoaderFactory(serializationService.toObject(cacheLoaderFactory));
        config.setCacheWriterFactory(serializationService.toObject(cacheWriterFactory));
        config.setExpiryPolicyFactory(serializationService.toObject(expiryPolicyFactory));
        config.setReadThrough(isReadThrough);
        config.setWriteThrough(isWriteThrough);
        config.setStoreByValue(isStoreByValue);
        config.setManagementEnabled(isManagementEnabled);
        config.setStatisticsEnabled(isStatisticsEnabled);
        config.setHotRestartConfig(hotRestartConfig);
        config.setEventJournalConfig(eventJournalConfig);
        config.setSplitBrainProtectionName(splitBrainProtectionName);

        if (listenerConfigurations != null && !listenerConfigurations.isEmpty()) {
            config.setListenerConfigurations();
            listenerConfigurations
                    .forEach(listener -> config.addCacheEntryListenerConfiguration(serializationService.toObject(listener)));
        }

        config.setMergePolicyConfig(mergePolicyConfig);
        config.setDisablePerEntryInvalidationEvents(disablePerEntryInvalidationEvents);
        if (merkleTreeConfigExists) {
            config.setMerkleTreeConfig(merkleTreeConfig);
        }

        if (cachePartitionLostListenerConfigs != null) {
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs = new ArrayList<>(
                    cachePartitionLostListenerConfigs.size());
            cachePartitionLostListenerConfigs.forEach(listenerConfigHolder -> partitionLostListenerConfigs
                    .add(listenerConfigHolder.asListenerConfig(serializationService)));
            config.setPartitionLostListenerConfigs(partitionLostListenerConfigs);
        }

        return config;
    }

    public static <K, V> CacheConfigHolder of(CacheConfig<K, V> config, SerializationService serializationService) {
        if (config == null) {
            return null;
        }

        List<Data> listenerConfigurations = null;
        Set<CacheEntryListenerConfiguration<K, V>> entryListenerConfigurations = config.getListenerConfigurations();
        if (entryListenerConfigurations != null && !entryListenerConfigurations.isEmpty()) {
            listenerConfigurations = new ArrayList<>(entryListenerConfigurations.size());
            final List<Data> configDatas = listenerConfigurations;
            entryListenerConfigurations.forEach(listenerConfig -> configDatas.add(serializationService.toData(listenerConfig)));
        }

        List<ListenerConfigHolder> cachePartitionLostListenerConfigs = null;
        List<CachePartitionLostListenerConfig> partitionLostListenerConfigs = config.getPartitionLostListenerConfigs();
        if (partitionLostListenerConfigs != null) {
            cachePartitionLostListenerConfigs = new ArrayList<>(partitionLostListenerConfigs.size());
            final List<ListenerConfigHolder> configs = cachePartitionLostListenerConfigs;
            partitionLostListenerConfigs
                    .forEach(listenerConfig -> configs.add(ListenerConfigHolder.of(listenerConfig, serializationService)));
        }

        return new CacheConfigHolder(config.getName(), config.getManagerPrefix(), config.getUriString(), config.getBackupCount(),
                config.getAsyncBackupCount(), config.getInMemoryFormat().name(),
                EvictionConfigHolder.of(config.getEvictionConfig(), serializationService), config.getWanReplicationRef(),
                config.getKeyClassName(), config.getValueClassName(), serializationService.toData(config.getCacheLoaderFactory()),
                serializationService.toData(config.getCacheWriterFactory()),
                serializationService.toData(config.getExpiryPolicyFactory()), config.isReadThrough(), config.isWriteThrough(),
                config.isStoreByValue(), config.isManagementEnabled(), config.isStatisticsEnabled(), config.getHotRestartConfig(),
                config.getEventJournalConfig(), config.getSplitBrainProtectionName(), listenerConfigurations,
                config.getMergePolicyConfig(), config.isDisablePerEntryInvalidationEvents(),
                cachePartitionLostListenerConfigs, config.getMerkleTreeConfig() != null, config.getMerkleTreeConfig());
    }

}
