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

package com.hazelcast.internal.config;

import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.WanReplicationRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Readonly version of {@link com.hazelcast.config.CacheSimpleConfig}
 */
public class CacheSimpleConfigReadOnly extends CacheSimpleConfig {

    public CacheSimpleConfigReadOnly(CacheSimpleConfig cacheSimpleConfig) {
        super(cacheSimpleConfig);
    }

    @Override
    public EvictionConfig getEvictionConfig() {
        final EvictionConfig evictionConfig = super.getEvictionConfig();
        if (evictionConfig == null) {
            return null;
        }
        return new EvictionConfigReadOnly(evictionConfig);
    }

    @Override
    public List<CacheSimpleEntryListenerConfig> getCacheEntryListeners() {
        final List<CacheSimpleEntryListenerConfig> listenerConfigs = super.getCacheEntryListeners();
        final List<CacheSimpleEntryListenerConfig> readOnlyListenerConfigs = new ArrayList<CacheSimpleEntryListenerConfig>(
                listenerConfigs.size());
        for (CacheSimpleEntryListenerConfig listenerConfig : listenerConfigs) {
            readOnlyListenerConfigs.add(new CacheSimpleEntryListenerConfigReadOnly(listenerConfig));
        }
        return Collections.unmodifiableList(readOnlyListenerConfigs);
    }

    @Override
    public CacheSimpleConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setCacheEntryListeners(List<CacheSimpleEntryListenerConfig> cacheEntryListeners) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setCacheLoaderFactory(String cacheLoaderFactory) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setCacheWriterFactory(String cacheWriterFactory) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setEvictionConfig(EvictionConfig evictionConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setExpiryPolicyFactoryConfig(ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setExpiryPolicyFactory(String className) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setKeyType(String keyType) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setManagementEnabled(boolean managementEnabled) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setReadThrough(boolean readThrough) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setValueType(String valueType) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setWriteThrough(boolean writeThrough) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig addEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig addCachePartitionLostListenerConfig(CachePartitionLostListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setPartitionLostListenerConfigs(
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setDisablePerEntryInvalidationEvents(boolean disablePerEntryInvalidationEvents) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheSimpleConfig setHotRestartConfig(HotRestartConfig hotRestartConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }
}
