/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configuration for a {@link com.hazelcast.crdt.pncounter.PNCounter}
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
    private String quorumName;
    private boolean statisticsEnabled = DEFAULT_STATISTICS_ENABLED;

    private transient PNCounterConfigReadOnly readOnly;

    public PNCounterConfig() {
    }

    public PNCounterConfig(String name, int replicaCount, String quorumName, boolean statisticsEnabled) {
        this.name = name;
        this.replicaCount = replicaCount;
        this.quorumName = quorumName;
        this.statisticsEnabled = statisticsEnabled;
    }

    public PNCounterConfig(String name) {
        this.name = name;
    }

    public PNCounterConfig(PNCounterConfig config) {
        this(config.getName(), config.getReplicaCount(), config.getQuorumName(), config.isStatisticsEnabled());
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
     * @return the updated PN counter config
     */
    public PNCounterConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /** Returns a read only instance of this config */
    PNCounterConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new PNCounterConfigReadOnly(this);
        }
        return readOnly;
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
        out.writeUTF(name);
        out.writeInt(replicaCount);
        out.writeBoolean(statisticsEnabled);
        out.writeUTF(quorumName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        replicaCount = in.readInt();
        statisticsEnabled = in.readBoolean();
        quorumName = in.readUTF();
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
        return quorumName != null ? quorumName.equals(that.quorumName) : that.quorumName == null;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + replicaCount;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        return result;
    }

    // not private for testing
    static class PNCounterConfigReadOnly extends PNCounterConfig {

        PNCounterConfigReadOnly(PNCounterConfig config) {
            super(config);
        }

        @Override
        public PNCounterConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only PN counter: " + getName());
        }

        @Override
        public PNCounterConfig setReplicaCount(int replicaCount) {
            throw new UnsupportedOperationException("This config is read-only PN counter: " + getName());
        }

        @Override
        public PNCounterConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only PN counter: " + getName());
        }

        @Override
        public PNCounterConfig setStatisticsEnabled(boolean statisticsEnabled) {
            throw new UnsupportedOperationException("This config is read-only PN counter: " + getName());
        }
    }

}
