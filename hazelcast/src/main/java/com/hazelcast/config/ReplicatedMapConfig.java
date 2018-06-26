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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.ReplicatedMap}
 */
@SuppressWarnings("checkstyle:methodcount")
public class ReplicatedMapConfig implements SplitBrainMergeTypeProvider, IdentifiedDataSerializable, Versioned {

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
    public static final String DEFAULT_MERGE_POLICY = PutIfAbsentMergePolicy.class.getName();

    private String name;
    // concurrencyLevel is deprecated and it's not used anymore
    // it's left just for backwards compatibility -> it's transient
    private transient int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    // replicationDelayMillis is deprecated, unused and hence transient
    private transient long replicationDelayMillis = DEFAULT_REPLICATION_DELAY_MILLIS;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // replicatorExecutorService is deprecated, unused and hence transient
    private transient ScheduledExecutorService replicatorExecutorService;
    private boolean asyncFillup = DEFAULT_ASNYC_FILLUP;
    private boolean statisticsEnabled = true;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    private List<ListenerConfig> listenerConfigs;

    private String quorumName;

    private transient volatile ReplicatedMapConfigReadOnly readOnly;

    public ReplicatedMapConfig() {
    }

    /**
     * Creates a ReplicatedMapConfig with the given name.
     *
     * @param name the name of the ReplicatedMap
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
        this.listenerConfigs = replicatedMapConfig.listenerConfigs == null ? null
                : new ArrayList<ListenerConfig>(replicatedMapConfig.getListenerConfigs());
        this.asyncFillup = replicatedMapConfig.asyncFillup;
        this.statisticsEnabled = replicatedMapConfig.statisticsEnabled;
        this.mergePolicyConfig = replicatedMapConfig.mergePolicyConfig;
        this.quorumName = replicatedMapConfig.quorumName;
    }

    /**
     * Returns the name of this {@link com.hazelcast.core.ReplicatedMap}.
     *
     * @return the name of the {@link com.hazelcast.core.ReplicatedMap}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this {@link com.hazelcast.core.ReplicatedMap}.
     *
     * @param name the name of the {@link com.hazelcast.core.ReplicatedMap}
     * @return the current replicated map config instance
     */
    public ReplicatedMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * The number of milliseconds after a put is executed before the value is replicated
     * to other nodes. During this time, multiple puts can be operated and cached up to be sent
     * out all at once after the delay.
     * <p>
     * The default value is 100ms before a replication is operated.
     * If set to 0, no delay is used and all values are replicated one by one.
     *
     * @return the number of milliseconds after a put is executed before the value is replicated to other nodes
     * @deprecated since new implementation will route puts to the partition owner nodes,
     * caching won't help replication speed because most of the time subsequent puts will end up in different nodes
     */
    @Deprecated
    public long getReplicationDelayMillis() {
        return replicationDelayMillis;
    }

    /**
     * Sets the number of milliseconds after a put is executed before the value is replicated
     * to other nodes. During this time, multiple puts can be operated and cached up to be sent
     * out all at once after the delay.
     * <p>
     * The default value is 100ms before a replication is operated.
     * If set to 0, no delay is used and all values are replicated one by one.
     *
     * @param replicationDelayMillis the number of milliseconds after a put is executed before the value is replicated
     *                               to other nodes
     * @return the current replicated map config instance
     * @deprecated since new implementation will route puts to the partition owner nodes,
     * caching won't help replication speed because most of the time subsequent puts will end up in different nodes
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
     * @return Number of parallel mutexes to minimize contention on keys
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
     * @param concurrencyLevel Number of parallel mutexes to minimize contention on keys
     * @return the current replicated map config instance
     * @deprecated new implementation doesn't use mutexes
     */
    @Deprecated
    public ReplicatedMapConfig setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    /**
     * Data type used to store entries.
     * <p>
     * Possible values:
     * <ul>
     * <li>BINARY: keys and values are stored as binary data</li>
     * <li>OBJECT (default): values are stored in their object forms</li>
     * <li>NATIVE: keys and values are stored in native memory</li>
     * </ul>
     *
     * @return Data type used to store entries
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type used to store entries.
     * <p>
     * Possible values:
     * <ul>
     * <li>BINARY: keys and values are stored as binary data</li>
     * <li>OBJECT (default): values are stored in their object forms</li>
     * <li>NATIVE: keys and values are stored in native memory</li>
     * </ul>
     *
     * @param inMemoryFormat Data type used to store entries
     * @return the current replicated map config instance
     */
    public ReplicatedMapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    /**
     * @deprecated new implementation doesn't use executor service for replication
     */
    @Deprecated
    public ScheduledExecutorService getReplicatorExecutorService() {
        return replicatorExecutorService;
    }

    /**
     * @deprecated new implementation doesn't use executor service for replication
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
     * @return {@code true} if the replicated map is available for reads before the initial
     * replication is completed, {@code false} otherwise
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
     * @param asyncFillup {@code true} if the replicated map is available for reads before the initial
     *                    replication is completed, {@code false} otherwise
     */
    public void setAsyncFillup(boolean asyncFillup) {
        this.asyncFillup = asyncFillup;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public ReplicatedMapConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ReplicatedMapConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Checks if statistics are enabled for this replicated map.
     *
     * @return {@code true} if statistics are enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Sets statistics to enabled or disabled for this replicated map.
     *
     * @param statisticsEnabled {@code true} to enable replicated map statistics, {@code false} to disable
     * @return the current replicated map config instance
     */
    public ReplicatedMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Returns the quorum name for operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for operations.
     *
     * @param quorumName the quorum name
     * @return the updated configuration
     */
    public ReplicatedMapConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /**
     * Gets the replicated map merge policy {@link com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy}
     *
     * @return the updated replicated map configuration
     * @deprecated since 3.10, please use {@link #getMergePolicyConfig()} and {@link MergePolicyConfig#getPolicy()}
     */
    public String getMergePolicy() {
        return mergePolicyConfig.getPolicy();
    }

    /**
     * Sets the replicated map merge policy {@link com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy}
     *
     * @param mergePolicy the replicated map merge policy to set
     * @return the updated replicated map configuration
     * @deprecated since 3.10, please use {@link #setMergePolicyConfig(MergePolicyConfig)}
     */
    public ReplicatedMapConfig setMergePolicy(String mergePolicy) {
        this.mergePolicyConfig.setPolicy(mergePolicy);
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this replicated map.
     *
     * @return the {@link MergePolicyConfig} for this replicated map
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this replicated map.
     *
     * @return the updated replicated map configuration
     */
    public ReplicatedMapConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null!");
        return this;
    }

    @Override
    public Class getProvidedMergeTypes() {
        return SplitBrainMergeTypes.ReplicatedMapMergeTypes.class;
    }

    @Override
    public String toString() {
        return "ReplicatedMapConfig{"
                + "name='" + name + '\''
                + "', inMemoryFormat=" + inMemoryFormat + '\''
                + ", concurrencyLevel=" + concurrencyLevel
                + ", replicationDelayMillis=" + replicationDelayMillis
                + ", asyncFillup=" + asyncFillup
                + ", statisticsEnabled=" + statisticsEnabled
                + ", quorumName='" + quorumName + '\''
                + ", mergePolicyConfig='" + mergePolicyConfig + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.REPLICATED_MAP_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(inMemoryFormat.name());
        out.writeBoolean(asyncFillup);
        out.writeBoolean(statisticsEnabled);
        // RU_COMPAT_3_9
        if (out.getVersion().isLessThan(Versions.V3_10)) {
            out.writeUTF(mergePolicyConfig.getPolicy());
        }
        writeNullableList(listenerConfigs, out);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeUTF(quorumName);
            out.writeObject(mergePolicyConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        inMemoryFormat = InMemoryFormat.valueOf(in.readUTF());
        asyncFillup = in.readBoolean();
        statisticsEnabled = in.readBoolean();
        // RU_COMPAT_3_9
        if (in.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            mergePolicyConfig.setPolicy(in.readUTF());
        }
        listenerConfigs = readNullableList(in);
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            quorumName = in.readUTF();
            mergePolicyConfig = in.readObject();
        }
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReplicatedMapConfig)) {
            return false;
        }

        ReplicatedMapConfig that = (ReplicatedMapConfig) o;
        if (asyncFillup != that.asyncFillup) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        if (mergePolicyConfig != null ? !mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig != null) {
            return false;
        }
        return listenerConfigs != null ? listenerConfigs.equals(that.listenerConfigs) : that.listenerConfigs == null;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (inMemoryFormat != null ? inMemoryFormat.hashCode() : 0);
        result = 31 * result + (asyncFillup ? 1 : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (listenerConfigs != null ? listenerConfigs.hashCode() : 0);
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }
}
