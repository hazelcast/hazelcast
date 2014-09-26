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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.Collections;
import java.util.Set;

/**
 * Contains the configuration for an {@link com.hazelcast.cache.ICache} (read-only).
 *
 * @param <K>
 * @param <V>
 */
public class CacheConfigReadOnly<K, V> extends CacheConfig<K, V> {

    CacheConfigReadOnly(CacheConfig config) {
        super(config);
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
    public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
        Iterable<CacheEntryListenerConfiguration<K, V>> listenerConfigurations = super.getCacheEntryListenerConfigurations();
        return Collections.unmodifiableSet((Set<CacheEntryListenerConfiguration<K, V>>) listenerConfigurations);
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
    public CacheConfig<K, V> setEvictionPolicy(final EvictionPolicy evictionPolicy) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig setNearCacheConfig(final NearCacheConfig nearCacheConfig) {
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
    public CacheConfig<K, V> setTypes(final Class<K> keyType, final Class<V> valueType) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public CacheConfig<K, V> setStoreByValue(final boolean storeByValue) {
        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
    }

    @Override
    public NearCacheConfig getNearCacheConfig() {
        final NearCacheConfig nearCacheConfig = super.getNearCacheConfig();
        if (nearCacheConfig == null) {
            return null;
        }
        return nearCacheConfig.getAsReadOnly();
    }
}
