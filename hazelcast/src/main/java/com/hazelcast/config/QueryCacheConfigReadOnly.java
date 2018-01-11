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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read only {@code QueryCacheConfig}
 *
 * @since 3.5
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
class QueryCacheConfigReadOnly extends QueryCacheConfig {

    public QueryCacheConfigReadOnly(QueryCacheConfig other) {
        super(other);
    }

    @Override
    public List<MapIndexConfig> getIndexConfigs() {
        List<MapIndexConfig> mapIndexConfigs = super.getIndexConfigs();
        List<MapIndexConfig> readOnlyMapIndexConfigs = new ArrayList<MapIndexConfig>(mapIndexConfigs.size());
        for (MapIndexConfig mapIndexConfig : mapIndexConfigs) {
            readOnlyMapIndexConfigs.add(mapIndexConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyMapIndexConfigs);
    }

    @Override
    public List<EntryListenerConfig> getEntryListenerConfigs() {
        List<EntryListenerConfig> listenerConfigs = super.getEntryListenerConfigs();
        List<EntryListenerConfig> readOnlyListenerConfigs = new ArrayList<EntryListenerConfig>(listenerConfigs.size());
        for (EntryListenerConfig listenerConfig : listenerConfigs) {
            readOnlyListenerConfigs.add(listenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyListenerConfigs);
    }

    @Override
    public EvictionConfig getEvictionConfig() {
        return super.getEvictionConfig().getAsReadOnly();
    }

    @Override
    public PredicateConfig getPredicateConfig() {
        return super.getPredicateConfig().getAsReadOnly();
    }

    @Override
    public QueryCacheConfig setBatchSize(int batchSize) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setBufferSize(int bufferSize) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setDelaySeconds(int delaySeconds) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setIncludeValue(boolean includeValue) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setIndexConfigs(List<MapIndexConfig> indexConfigs) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setPredicateConfig(PredicateConfig predicateConfig) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setPopulate(boolean populate) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }

    @Override
    public QueryCacheConfig setCoalesce(boolean coalesce) {
        throw new UnsupportedOperationException("This config is read-only query cache: " + getName());
    }
}
