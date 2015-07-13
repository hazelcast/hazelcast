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

import com.hazelcast.replicatedmap.merge.PutIfAbsentMapMergePolicy;
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
    /**
     * Default policy for merging
     */
    public static final String DEFAULT_MERGE_POLICY = PutIfAbsentMapMergePolicy.class.getName();


    private String name;
    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    private long replicationDelayMillis = DEFAULT_REPLICATION_DELAY_MILLIS;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private ScheduledExecutorService replicatorExecutorService;
    private boolean asyncFillup = DEFAULT_ASNYC_FILLUP;
    private boolean statisticsEnabled = true;
    private String mergePolicy = DEFAULT_MERGE_POLICY;


    private List<ListenerConfig> listenerConfigs;

    public ReplicatedMapConfig() {
    }

    /**
     * Creates a ReplicatedMapConfig with the given name.
     *
     * @param name the name of the ReplicatedMap.
     */
    public ReplicatedMapConfig(String name) {
        setName(name);
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
        this.mergePolicy = replicatedMapConfig.mergePolicy;
    }

    /**
     * Returns the name of this {@link com.hazelcast.core.ReplicatedMap}.
     *
     * @return the name of the {@link com.hazelcast.core.ReplicatedMap}.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this {@link com.hazelcast.core.ReplicatedMap}.
     *
     * @param name the name of the {@link com.hazelcast.core.ReplicatedMap}.
     * @return The current replicated map config instance.
     */
    public ReplicatedMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * The number of milliseconds after a put is executed before the value is replicated
     * to other nodes. During this time, multiple puts can be operated and cached up to be sent
     * out all at once after the delay.
     * Default value is 100ms before a replication is operated. If 0, no delay is used and
     * all values are replicated one by one.
     *
     * @return the number of milliseconds after a put is executed before the value is replicated
     * to other nodes.
     * @deprecated since new implementation will route puts to the partition owner nodes,
     * caching won't help replication speed because most of the time subsequent puts will end up in different nodes.
     */
    @Deprecated
    public long getReplicationDelayMillis() {
        return replicationDelayMillis;
    }

    /**
     * Sets the number of milliseconds after a put is executed before the value is replicated
     * to other nodes. During this time, multiple puts can be operated and cached up to be sent
     * out all at once after the delay.
     * Default value is 100ms before a replication is operated. If set to 0, no delay is used and
     * all values are replicated one by one.
     *
     * @param replicationDelayMillis the number of milliseconds after a put is executed before the value is replicated
     *                               to other nodes.
     * @return The current replicated map config instance.
     * @deprecated since new implementation will route puts to the partition owner nodes,
     * caching won't help replication speed because most of the time subsequent puts will end up in different nodes.
     */
    @Deprecated
    public ReplicatedMapConfig setReplicationDelayMillis(long replicationDelayMillis) {
        this.replicationDelayMillis = replicationDelayMillis;
        return this;
    }

    /**
     * Number of parallel mutexes to minimize contention on keys. The default value is 32 which
     * is a good number for lots of applications. If higher contention is seen on writes to values
     * inside of the replicated map, this value can be adjusted to the needs.
     *
     * @return Number of parallel mutexes to minimize contention on keys.
     * @deprecated new implementation doesn't use mutexes
     */
    @Deprecated
    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    /**
     * Sets the number of parallel mutexes to minimize contention on keys. The default value is 32 which
     * is a good number for lots of applications. If higher contention is seen on writes to values
     * inside of the replicated map, this value can be adjusted to the needs.
     *
     * @param concurrencyLevel Number of parallel mutexes to minimize contention on keys.
     * @return The current replicated map config instance.
     * @deprecated new implementation doesn't use mutexes
     */
    @Deprecated
    public ReplicatedMapConfig setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    /**
     * Data type used to store entries.
     * Possible values:
     * BINARY: keys and values are stored as binary data.
     * OBJECT (default): values are stored in their object forms.
     * NATIVE: keys and values are stored in native memory.
     *
     * @return Data type used to store entries.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type used to store entries.
     * Possible values:
     * BINARY: keys and values are stored as binary data.
     * OBJECT (default): values are stored in their object forms.
     * NATIVE: keys and values are stored in native memory.
     *
     * @param inMemoryFormat Data type used to store entries.
     * @return The current replicated map config instance.
     */
    public ReplicatedMapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    /**
     * @deprecated new implementation doesn't use executor service for replication.
     */
    @Deprecated
    public ScheduledExecutorService getReplicatorExecutorService() {
        return replicatorExecutorService;
    }

    /**
     * @deprecated new implementation doesn't use executor service for replication.
     */
    @Deprecated
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

    /**
     * True if the replicated map is available for reads before the initial
     * replication is completed, false otherwise. Default is true. If false, no Exception will be
     * thrown when the replicated map is not yet ready, but `null` values can be seen until
     * the initial replication is completed.
     *
     * @return True if the replicated map is available for reads before the initial
     * replication is completed, false otherwise.
     */
    public boolean isAsyncFillup() {
        return asyncFillup;
    }

    /**
     * True if the replicated map is available for reads before the initial
     * replication is completed, false otherwise. Default is true. If false, no Exception will be
     * thrown when the replicated map is not yet ready, but `null` values can be seen until
     * the initial replication is completed.
     *
     * @param asyncFillup True if the replicated map is available for reads before the initial
     * replication is completed, false otherwise.
     */
    public void setAsyncFillup(boolean asyncFillup) {
        this.asyncFillup = asyncFillup;
    }

    public ReplicatedMapConfig getAsReadOnly() {
        return new ReplicatedMapConfigReadOnly(this);
    }

    /**
     * Checks if statistics are enabled for this replicated map.
     *
     * @return True if statistics are enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Sets statistics to enabled or disabled for this replicated map.
     *
     * @param statisticsEnabled True to enable replicated map statistics, false to disable.
     * @return The current replicated map config instance.
     */
    public ReplicatedMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }


    /**
     * Gets the replicated map merge policy {@link com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy}
     *
     * @return the updated replicated map configuration
     */
    public String getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Sets the replicated map merge policy {@link com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy}
     *
     * @param mergePolicy the replicated map merge policy to set
     * @return the updated replicated map configuration
     */
    public ReplicatedMapConfig setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }


}
