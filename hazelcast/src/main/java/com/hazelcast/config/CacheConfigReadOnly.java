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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Readonly version of {@link com.hazelcast.config.CacheConfig}
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class CacheConfigReadOnly<K, V> extends CacheConfig<K, V> {

    CacheConfigReadOnly(CacheConfig config) {
        super(config);
    }

    // TODO Change to "EvictionConfig" instead of "CacheEvictionConfig" in the future
    // since "CacheEvictionConfig" is deprecated
    @Override
    public CacheEvictionConfig getEvictionConfig() {
        final CacheEvictionConfig evictionConfig = super.getEvictionConfig();
        if (evictionConfig == null) {
            return null;
        }
        return (CacheEvictionConfig) evictionConfig.getAsReadOnly();
    }

    @Override
    public WanReplicationRef getWanReplicationRef() {
        final WanReplicationRef wanReplicationRef = super.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return null;
        }
        return wanReplicationRef.getAsReadOnly();
    }

    @Override
    public String getQuorumName() {
        return super.getQuorumName();
    }

    @Override
    public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
        Iterable<CacheEntryListenerConfiguration<K, V>> listenerConfigurations = super.getCacheEntryListenerConfigurations();
        return Collections.unmodifiableSet((Set<CacheEntryListenerConfiguration<K, V>>) listenerConfigurations);
    }

    @Override
    public CacheConfig<K, V> addCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> removeCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setName(final String name) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setManagerPrefix(final String managerPrefix) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setUriString(final String uriString) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setBackupCount(final int backupCount) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setAsyncBackupCount(final int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setEvictionConfig(final EvictionConfig evictionConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setInMemoryFormat(final InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setManagementEnabled(final boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setTypes(final Class<K> keyType, final Class<V> valueType) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setStoreByValue(final boolean storeByValue) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig setWanReplicationRef(final WanReplicationRef wanReplicationRef) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig setQuorumName(String quorumName) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setHotRestartConfig(HotRestartConfig hotRestartConfig) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig setPartitionLostListenerConfigs(
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public void setMergePolicy(String mergePolicy) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> expiryPolicyFactory) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> cacheLoaderFactory) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setCacheWriterFactory(
            Factory<? extends CacheWriter<? super K, ? super V>> cacheWriterFactory) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfiguration<K, V> setReadThrough(boolean isReadThrough) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

}
