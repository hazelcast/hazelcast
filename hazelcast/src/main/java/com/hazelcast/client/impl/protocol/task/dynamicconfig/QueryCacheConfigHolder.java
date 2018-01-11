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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.ArrayList;
import java.util.List;

/**
 * Client protocol adapter for QueryCacheConfig
 */
public class QueryCacheConfigHolder {
    private int batchSize;
    private int bufferSize;
    private int delaySeconds;
    private boolean includeValue;
    private boolean populate;
    private boolean coalesce;
    private String inMemoryFormat;
    private String name;
    private PredicateConfigHolder predicateConfigHolder;
    private EvictionConfigHolder evictionConfigHolder;
    private List<ListenerConfigHolder> listenerConfigs;
    private List<MapIndexConfig> indexConfigs;

    public QueryCacheConfigHolder() {
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getDelaySeconds() {
        return delaySeconds;
    }

    public void setDelaySeconds(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public void setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
    }

    public boolean isPopulate() {
        return populate;
    }

    public void setPopulate(boolean populate) {
        this.populate = populate;
    }

    public boolean isCoalesce() {
        return coalesce;
    }

    public void setCoalesce(boolean coalesce) {
        this.coalesce = coalesce;
    }

    public String getInMemoryFormat() {
        return inMemoryFormat;
    }

    public void setInMemoryFormat(String inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PredicateConfigHolder getPredicateConfigHolder() {
        return predicateConfigHolder;
    }

    public void setPredicateConfigHolder(PredicateConfigHolder predicateConfigHolder) {
        this.predicateConfigHolder = predicateConfigHolder;
    }

    public EvictionConfigHolder getEvictionConfigHolder() {
        return evictionConfigHolder;
    }

    public void setEvictionConfigHolder(EvictionConfigHolder evictionConfigHolder) {
        this.evictionConfigHolder = evictionConfigHolder;
    }

    public List<ListenerConfigHolder> getListenerConfigs() {
        return listenerConfigs;
    }

    public void setListenerConfigs(List<ListenerConfigHolder> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
    }

    public List<MapIndexConfig> getIndexConfigs() {
        return indexConfigs;
    }

    public void setIndexConfigs(List<MapIndexConfig> indexConfigs) {
        this.indexConfigs = indexConfigs;
    }

    public QueryCacheConfig asQueryCacheConfig(SerializationService serializationService) {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBatchSize(batchSize);
        config.setBufferSize(bufferSize);
        config.setCoalesce(coalesce);
        config.setDelaySeconds(delaySeconds);
        config.setEvictionConfig(evictionConfigHolder.asEvictionConfg(serializationService));
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            List<EntryListenerConfig> entryListenerConfigs = new ArrayList<EntryListenerConfig>(listenerConfigs.size());
            for (ListenerConfigHolder holder : listenerConfigs) {
                entryListenerConfigs.add((EntryListenerConfig) holder.asListenerConfig(serializationService));
            }
            config.setEntryListenerConfigs(entryListenerConfigs);
        } else {
            config.setEntryListenerConfigs(new ArrayList<EntryListenerConfig>());
        }
        config.setIncludeValue(includeValue);
        config.setInMemoryFormat(InMemoryFormat.valueOf(inMemoryFormat));
        config.setIndexConfigs(indexConfigs == null ? new ArrayList<MapIndexConfig>() : indexConfigs);
        config.setName(name);
        config.setPredicateConfig(predicateConfigHolder.asPredicateConfig(serializationService));
        config.setPopulate(populate);
        return config;
    }

    public static QueryCacheConfigHolder of(QueryCacheConfig config, SerializationService serializationService) {
        QueryCacheConfigHolder holder = new QueryCacheConfigHolder();
        holder.setBatchSize(config.getBatchSize());
        holder.setBufferSize(config.getBufferSize());
        holder.setCoalesce(config.isCoalesce());
        holder.setDelaySeconds(config.getDelaySeconds());
        holder.setEvictionConfigHolder(EvictionConfigHolder.of(config.getEvictionConfig(), serializationService));
        holder.setIncludeValue(config.isIncludeValue());
        holder.setInMemoryFormat(config.getInMemoryFormat().toString());
        holder.setName(config.getName());
        if (config.getIndexConfigs() != null && !config.getIndexConfigs().isEmpty()) {
            List<MapIndexConfig> indexConfigs = new ArrayList<MapIndexConfig>(config.getIndexConfigs().size());
            for (MapIndexConfig indexConfig : config.getIndexConfigs()) {
                indexConfigs.add(new MapIndexConfig(indexConfig));
            }
            holder.setIndexConfigs(indexConfigs);
        }
        if (config.getEntryListenerConfigs() != null && !config.getEntryListenerConfigs().isEmpty()) {
            List<ListenerConfigHolder> listenerConfigHolders =
                    new ArrayList<ListenerConfigHolder>(config.getEntryListenerConfigs().size());
            for (EntryListenerConfig listenerConfig : config.getEntryListenerConfigs()) {
                listenerConfigHolders.add(ListenerConfigHolder.of(listenerConfig, serializationService));
            }
            holder.setListenerConfigs(listenerConfigHolders);
        }
        holder.setPredicateConfigHolder(PredicateConfigHolder.of(config.getPredicateConfig(), serializationService));
        holder.setPopulate(config.isPopulate());
        return holder;
    }

}
