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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for an {@link ReplicatedMap}
 */
@SuppressWarnings("checkstyle:methodcount")
public class ReplicatedMapConfig implements IdentifiedDataSerializable, NamedConfig {

    /**
     * Default value of In-memory format
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.OBJECT;
    /**
     * Default value of asynchronous fill up
     */
    public static final boolean DEFAULT_ASNYC_FILLUP = true;

    private boolean statisticsEnabled = true;
    private boolean asyncFillup = DEFAULT_ASNYC_FILLUP;
    private String name;
    private String splitBrainProtectionName;
    private List<ListenerConfig> listenerConfigs = new ArrayList<>();
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

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
        this.listenerConfigs = replicatedMapConfig.listenerConfigs == null ? null
                : new ArrayList<>(replicatedMapConfig.getListenerConfigs());
        this.asyncFillup = replicatedMapConfig.asyncFillup;
        this.statisticsEnabled = replicatedMapConfig.statisticsEnabled;
        this.mergePolicyConfig = new MergePolicyConfig(replicatedMapConfig.mergePolicyConfig);
        this.splitBrainProtectionName = replicatedMapConfig.splitBrainProtectionName;
    }

    /**
     * Returns the name of this {@link ReplicatedMap}.
     *
     * @return the name of the {@link ReplicatedMap}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this {@link ReplicatedMap}.
     *
     * @param name the name of the {@link ReplicatedMap}
     * @return the current replicated map config instance
     */
    public ReplicatedMapConfig setName(String name) {
        this.name = name;
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

    public List<ListenerConfig> getListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<>();
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
     * @return this configuration
     */
    public ReplicatedMapConfig setAsyncFillup(boolean asyncFillup) {
        this.asyncFillup = asyncFillup;
        return this;
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
     * Returns the split brain protection name for operations.
     *
     * @return the split brain protection name
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated configuration
     */
    public ReplicatedMapConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
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
    public String toString() {
        return "ReplicatedMapConfig{"
                + "name='" + name + '\''
                + "', inMemoryFormat=" + inMemoryFormat + '\''
                + ", asyncFillup=" + asyncFillup
                + ", statisticsEnabled=" + statisticsEnabled
                + ", splitBrainProtectionName='" + splitBrainProtectionName + '\''
                + ", mergePolicyConfig='" + mergePolicyConfig + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.REPLICATED_MAP_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(inMemoryFormat.name());
        out.writeBoolean(asyncFillup);
        out.writeBoolean(statisticsEnabled);
        writeNullableList(listenerConfigs, out);
        out.writeString(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        inMemoryFormat = InMemoryFormat.valueOf(in.readString());
        asyncFillup = in.readBoolean();
        statisticsEnabled = in.readBoolean();
        listenerConfigs = readNullableList(in);
        splitBrainProtectionName = in.readString();
        mergePolicyConfig = in.readObject();
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
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (!Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)) {
            return false;
        }
        if (!Objects.equals(mergePolicyConfig, that.mergePolicyConfig)) {
            return false;
        }
        return Objects.equals(listenerConfigs, that.listenerConfigs);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (inMemoryFormat != null ? inMemoryFormat.hashCode() : 0);
        result = 31 * result + (asyncFillup ? 1 : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (listenerConfigs != null ? listenerConfigs.hashCode() : 0);
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }
}
