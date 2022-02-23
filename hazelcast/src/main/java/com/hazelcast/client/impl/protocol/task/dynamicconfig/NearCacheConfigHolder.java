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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.serialization.SerializationService;

public class NearCacheConfigHolder {

    private final String name;
    private final String inMemoryFormat;
    private final boolean serializeKeys;
    private final boolean invalidateOnChange;
    private final int timeToLiveSeconds;
    private final int maxIdleSeconds;
    private final EvictionConfigHolder evictionConfigHolder;
    private final boolean cacheLocalEntries;
    private final String localUpdatePolicy;
    private final NearCachePreloaderConfig preloaderConfig;

    public NearCacheConfigHolder(String name, String inMemoryFormat, boolean serializeKeys,
                                 boolean invalidateOnChange, int timeToLiveSeconds, int maxIdleSeconds,
                                 EvictionConfigHolder evictionConfigHolder, boolean cacheLocalEntries,
                                 String localUpdatePolicy, NearCachePreloaderConfig preloaderConfig) {
        this.name = name;
        this.inMemoryFormat = inMemoryFormat;
        this.serializeKeys = serializeKeys;
        this.invalidateOnChange = invalidateOnChange;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.evictionConfigHolder = evictionConfigHolder;
        this.cacheLocalEntries = cacheLocalEntries;
        this.localUpdatePolicy = localUpdatePolicy;
        this.preloaderConfig = preloaderConfig;
    }

    public String getName() {
        return name;
    }

    public String getInMemoryFormat() {
        return inMemoryFormat;
    }

    public boolean isSerializeKeys() {
        return serializeKeys;
    }

    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public EvictionConfigHolder getEvictionConfigHolder() {
        return evictionConfigHolder;
    }

    public boolean isCacheLocalEntries() {
        return cacheLocalEntries;
    }

    public String getLocalUpdatePolicy() {
        return localUpdatePolicy;
    }

    public NearCachePreloaderConfig getPreloaderConfig() {
        return preloaderConfig;
    }

    public NearCacheConfig asNearCacheConfig(SerializationService serializationService) {
        NearCacheConfig config = new NearCacheConfig();
        config.setName(name);
        config.setInMemoryFormat(inMemoryFormat);
        config.setSerializeKeys(serializeKeys);
        config.setInvalidateOnChange(invalidateOnChange);
        config.setTimeToLiveSeconds(timeToLiveSeconds);
        config.setMaxIdleSeconds(maxIdleSeconds);
        config.setEvictionConfig(evictionConfigHolder.asEvictionConfig(serializationService));
        config.setCacheLocalEntries(cacheLocalEntries);
        config.setLocalUpdatePolicy(LocalUpdatePolicy.valueOf(localUpdatePolicy));
        config.setPreloaderConfig(preloaderConfig);
        return config;
    }

    public static NearCacheConfigHolder of(NearCacheConfig config, SerializationService serializationService) {
        if (config == null) {
            return null;
        }
        return new NearCacheConfigHolder(config.getName(), config.getInMemoryFormat().name(), config.isSerializeKeys(),
                config.isInvalidateOnChange(), config.getTimeToLiveSeconds(), config.getMaxIdleSeconds(),
                EvictionConfigHolder.of(config.getEvictionConfig(), serializationService),
                config.isCacheLocalEntries(), config.getLocalUpdatePolicy().name(), config.getPreloaderConfig());
    }
}
