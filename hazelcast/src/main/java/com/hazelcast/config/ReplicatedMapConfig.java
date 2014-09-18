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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
/**
 * Contains the configuration for an {@link com.hazelcast.core.ReplicatedMap}
 */
public class ReplicatedMapConfig {

    /**
     * Default value of concurrency level
     */
    public static final int DEFAULT_CONCURRENCY_LEVEL = 32;
    /**
     * Default value of delay of replication in millisecond
     */
    public static final int DEFAULT_REPLICATION_DELAY_MILLIS = 100;
    /**
     * Default value of In-memory format
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.OBJECT;
    /**
     * Default value of asynchronous fill up
     */
    public static final boolean DEFAULT_ASNYC_FILLUP = true;

    private String name;
    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    private long replicationDelayMillis = DEFAULT_REPLICATION_DELAY_MILLIS;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private ScheduledExecutorService replicatorExecutorService;
    private boolean asyncFillup = DEFAULT_ASNYC_FILLUP;
    private boolean statisticsEnabled = true;

    private List<ListenerConfig> listenerConfigs;

    public ReplicatedMapConfig() {
    }

    public ReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        this.name = replicatedMapConfig.name;
        this.inMemoryFormat = replicatedMapConfig.inMemoryFormat;
        this.concurrencyLevel = replicatedMapConfig.concurrencyLevel;
        this.replicationDelayMillis = replicatedMapConfig.replicationDelayMillis;
        this.replicatorExecutorService = replicatedMapConfig.replicatorExecutorService;
        this.listenerConfigs = new ArrayList<ListenerConfig>(replicatedMapConfig.getListenerConfigs());
        this.asyncFillup = replicatedMapConfig.asyncFillup;
        this.statisticsEnabled = replicatedMapConfig.statisticsEnabled;
    }

    public String getName() {
        return name;
    }

    public ReplicatedMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    public long getReplicationDelayMillis() {
        return replicationDelayMillis;
    }

    public ReplicatedMapConfig setReplicationDelayMillis(long replicationDelayMillis) {
        this.replicationDelayMillis = replicationDelayMillis;
        return this;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public ReplicatedMapConfig setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public ReplicatedMapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    public ScheduledExecutorService getReplicatorExecutorService() {
        return replicatorExecutorService;
    }

    public ReplicatedMapConfig setReplicatorExecutorService(ScheduledExecutorService replicatorExecutorService) {
        this.replicatorExecutorService = replicatorExecutorService;
        return this;
    }

    public List<ListenerConfig> getListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ListenerConfig>();
        }
        return listenerConfigs;
    }

    public ReplicatedMapConfig setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public ReplicatedMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    public boolean isAsyncFillup() {
        return asyncFillup;
    }

    public void setAsyncFillup(boolean asyncFillup) {
        this.asyncFillup = asyncFillup;
    }

    public ReplicatedMapConfig getAsReadOnly() {
        return new ReplicatedMapConfigReadOnly(this);
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public ReplicatedMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

}
