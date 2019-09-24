/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CacheConfigHolder {
    private String name;
    private String managerPrefix;
    private String uriString;
    private int backupCount;
    private int asyncBackupCount;
    private String inMemoryFormat;
    private EvictionConfigHolder evictionConfigHolder;
    private WanReplicationRef wanReplicationRef;
    private String keyClassName;
    private String valueClassName;
    private Data cacheLoaderFactory;
    private Data cacheWriterFactory;
    private Data expiryPolicyFactory;
    private boolean isReadThrough;
    private boolean isWriteThrough;
    private boolean isStoreByValue;
    private boolean isManagementEnabled;
    private boolean isStatisticsEnabled;
    private HotRestartConfig hotRestartConfig;
    private EventJournalConfig eventJournalConfig;
    private String splitBrainProtectionName;
    private List<Data> listenerConfigurations;
    private MergePolicyConfig mergePolicyConfig;
    private boolean disablePerEntryInvalidationEvents;

    public CacheConfigHolder(String name, String managerPrefix, String uriString, int backupCount, int asyncBackupCount,
                             String inMemoryFormat, EvictionConfigHolder evictionConfigHolder,
                             WanReplicationRef wanReplicationRef, String keyClassName, String valueClassName,
                             Data cacheLoaderFactory, Data cacheWriterFactory, Data expiryPolicyFactory, boolean isReadThrough,
                             boolean isWriteThrough, boolean isStoreByValue, boolean isManagementEnabled,
                             boolean isStatisticsEnabled, HotRestartConfig hotRestartConfig,
                             EventJournalConfig eventJournalConfig, String splitBrainProtectionName,
                             List<Data> listenerConfigurations, MergePolicyConfig mergePolicyConfig,
                             boolean disablePerEntryInvalidationEvents) {
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

    public <K, V> CacheConfig<K, V> asCacheConfig(SerializationService serializationService) {
        CacheConfig<K, V> config = new CacheConfig();
        config.setName(name);
        config.setManagerPrefix(managerPrefix);
        config.setUriString(uriString);
        config.setBackupCount(backupCount);
        config.setAsyncBackupCount(asyncBackupCount);
        config.setInMemoryFormat(InMemoryFormat.valueOf(inMemoryFormat));
        config.setEvictionConfig(evictionConfigHolder.asEvictionConfg(serializationService));
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

        return new CacheConfigHolder(config.getName(), config.getManagerPrefix(), config.getUriString(), config.getBackupCount(),
                config.getAsyncBackupCount(), config.getInMemoryFormat().name(),
                EvictionConfigHolder.of(config.getEvictionConfig(), serializationService), config.getWanReplicationRef(),
                config.getKeyClassName(), config.getValueClassName(), serializationService.toData(config.getCacheLoaderFactory()),
                serializationService.toData(config.getCacheWriterFactory()),
                serializationService.toData(config.getExpiryPolicyFactory()), config.isReadThrough(), config.isWriteThrough(),
                config.isStoreByValue(), config.isManagementEnabled(), config.isStatisticsEnabled(), config.getHotRestartConfig(),
                config.getEventJournalConfig(), config.getSplitBrainProtectionName(), listenerConfigurations,
                config.getMergePolicyConfig(), config.isDisablePerEntryInvalidationEvents());
    }

}
