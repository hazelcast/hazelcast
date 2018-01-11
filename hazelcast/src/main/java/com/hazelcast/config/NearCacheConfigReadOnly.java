/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains configuration for a Near Cache (read-only).
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class NearCacheConfigReadOnly extends NearCacheConfig {

    public NearCacheConfigReadOnly() {
    }

    public NearCacheConfigReadOnly(NearCacheConfig config) {
        super(config);
    }

    @Override
    public NearCacheConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setMaxSize(int maxSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setSerializeKeys(boolean serializeKeys) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setCacheLocalEntries(boolean cacheLocalEntries) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setLocalUpdatePolicy(LocalUpdatePolicy localUpdatePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public EvictionConfig getEvictionConfig() {
        return super.getEvictionConfig().getAsReadOnly();
    }

    @Override
    public NearCacheConfig setPreloaderConfig(NearCachePreloaderConfig preloaderConfig) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public NearCachePreloaderConfig getPreloaderConfig() {
        return super.getPreloaderConfig().getAsReadOnly();
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("NearCacheConfigReadOnly is not serializable");
    }
}
