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

import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for a {@link PNCounter}
 *
 * @since 3.10
 */
public class PNCounterConfig implements IdentifiedDataSerializable, NamedConfig {
    /**
     * The default number of replicas on which state for this CRDT is kept
     */
    public static final int DEFAULT_REPLICA_COUNT = Integer.MAX_VALUE;

    /**
     * Default value for statistics enabled.
     */
    public static final boolean DEFAULT_STATISTICS_ENABLED = true;

    private String name = "default";
    private int replicaCount = DEFAULT_REPLICA_COUNT;
    private String splitBrainProtectionName;
    private boolean statisticsEnabled = DEFAULT_STATISTICS_ENABLED;

    public PNCounterConfig() {
    }

    public PNCounterConfig(String name, int replicaCount, String splitBrainProtectionName, boolean statisticsEnabled) {
        this.name = name;
        this.replicaCount = replicaCount;
        this.splitBrainProtectionName = splitBrainProtectionName;
        this.statisticsEnabled = statisticsEnabled;
    }

    public PNCounterConfig(String name) {
        this.name = name;
    }

    public PNCounterConfig(PNCounterConfig config) {
        this(config.getName(), config.getReplicaCount(), config.getSplitBrainProtectionName(), config.isStatisticsEnabled());
    }

    /** Gets the name of the PN counter. */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the PN counter.
     *
     * @param name the name of the PN counter
     * @return the updated PN counter config
     */
    public PNCounterConfig setName(String name) {
        checkNotNull(name);
        this.name = name;
        return this;
    }

    /**
     * Checks if statistics are enabled for this PN counter
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this PN counter
     *
     * @param statisticsEnabled {@code true} to enable statistics, {@code false} to disable
     * @return the updated PN counter config
     */
    public PNCounterConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Returns the number of replicas on which state for this PN counter will
     * be kept. This number applies in quiescent state, if there are currently
     * membership changes or clusters are merging, the state may be temporarily
     * kept on more replicas.
     *
     * @return the number of replicas for the CRDT state
     */
    public int getReplicaCount() {
        return replicaCount;
    }

    /**
     * Sets the number of replicas on which state for this PN counter will
     * be kept. This number applies in quiescent state, if there are currently
     * membership changes or clusters are merging, the state may be temporarily
     * kept on more replicas.
     * <p>
     * The provided {@code replicaCount} must be between 1 and
     * {@value Integer#MAX_VALUE}.
     *
     * @param replicaCount the number of replicas for the CRDT state
     * @return the updated PN counter config
     * @throws InvalidConfigurationException if the {@code replicaCount} is less than 1
     */
    public PNCounterConfig setReplicaCount(int replicaCount) {
        if (replicaCount < 1) {
            throw new InvalidConfigurationException("Replica count must be greater or equal to 1");
        }
        this.replicaCount = replicaCount;
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
     * @return the updated PN counter config
     */
    public PNCounterConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.PN_COUNTER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(replicaCount);
        out.writeBoolean(statisticsEnabled);
        out.writeString(splitBrainProtectionName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        replicaCount = in.readInt();
        statisticsEnabled = in.readBoolean();
        splitBrainProtectionName = in.readString();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PNCounterConfig that = (PNCounterConfig) o;

        if (replicaCount != that.replicaCount) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        return splitBrainProtectionName != null ? splitBrainProtectionName.equals(that.splitBrainProtectionName)
                : that.splitBrainProtectionName == null;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + replicaCount;
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        return result;
    }
}
