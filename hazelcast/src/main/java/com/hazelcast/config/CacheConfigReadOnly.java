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

package com.hazelcast.config;

import com.hazelcast.nio.serialization.BinaryInterface;

import javax.annotation.Nonnull;
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
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
@BinaryInterface
public class CacheConfigReadOnly<K, V> extends CacheConfig<K, V> {

    CacheConfigReadOnly(CacheConfig config) {
        super(config);
    }

    @Override
    public EvictionConfig getEvictionConfig() {
        final EvictionConfig evictionConfig = super.getEvictionConfig();
        if (evictionConfig == null) {
            return null;
        }
        return evictionConfig.getAsReadOnly();
    }

    @Override
    public WanReplicationRef getWanReplicationRef() {
        final WanReplicationRef wanReplicationRef = super.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return null;
        }
        return wanReplicationRef.getAsReadOnly();
    }

    @Nonnull
    @Override
    public HotRestartConfig getHotRestartConfig() {
        HotRestartConfig hotRestartConfig = super.getHotRestartConfig();
        return hotRestartConfig.getAsReadOnly();
    }

    @Nonnull
    @Override
    public EventJournalConfig getEventJournalConfig() {
        EventJournalConfig eventJournalConfig = super.getEventJournalConfig();
        return eventJournalConfig.getAsReadOnly();
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
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> removeCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setName(final String name) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setManagerPrefix(final String managerPrefix) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setUriString(final String uriString) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setBackupCount(final int backupCount) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setAsyncBackupCount(final int asyncBackupCount) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setEvictionConfig(final EvictionConfig evictionConfig) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setInMemoryFormat(final InMemoryFormat inMemoryFormat) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setManagementEnabled(final boolean enabled) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setTypes(final Class<K> keyType, final Class<V> valueType) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setStoreByValue(final boolean storeByValue) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setWanReplicationRef(final WanReplicationRef wanReplicationRef) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setQuorumName(String quorumName) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setHotRestartConfig(@Nonnull HotRestartConfig hotRestartConfig) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setPartitionLostListenerConfigs(
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfig<K, V> setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> expiryPolicyFactory) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> cacheLoaderFactory) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setCacheWriterFactory(
            Factory<? extends CacheWriter<? super K, ? super V>> cacheWriterFactory) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setEventJournalConfig(@Nonnull EventJournalConfig eventJournalConfig) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough) {
        throw throwReadOnly();
    }

    @Override
    public CacheConfiguration<K, V> setReadThrough(boolean isReadThrough) {
        throw throwReadOnly();
    }

    @Override
    public void setDisablePerEntryInvalidationEvents(boolean disablePerEntryInvalidationEvents) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
