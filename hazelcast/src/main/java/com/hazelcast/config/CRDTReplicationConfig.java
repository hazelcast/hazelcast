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

import java.util.Objects;

/**
 * Configures the replication mechanism for all
 * {@link com.hazelcast.internal.crdt.CRDT} implementations.
 * The CRDT states are replicated in rounds (the period is configurable)
 * and in each round the state is replicated up to the configured number
 * of members.
 */
public class CRDTReplicationConfig {
    /**
     * The default period between two CRDT replication rounds.
     */
    public static final int DEFAULT_REPLICATION_PERIOD_MILLIS = 1000;
    /**
     * The default maximum number of members to which the CRDT states are
     * replicated in a single round.
     */
    public static final int DEFAULT_MAX_CONCURRENT_REPLICATION_TARGETS = 1;

    private int replicationPeriodMillis = DEFAULT_REPLICATION_PERIOD_MILLIS;
    private int maxConcurrentReplicationTargets = DEFAULT_MAX_CONCURRENT_REPLICATION_TARGETS;


    /**
     * Returns the period between two replications of CRDT states in
     * milliseconds.
     */
    public int getReplicationPeriodMillis() {
        return replicationPeriodMillis;
    }

    /**
     * Sets the period between two replications of CRDT states in milliseconds.
     * A lower value will increase the speed at which changes are disseminated
     * to other cluster members at the expense of burst-like behaviour - less
     * updates will be batched together in one replication message and one
     * update to a CRDT may cause a sudden burst of replication messages in a
     * short time interval.
     * The value must be a positive non-null integer.
     *
     * @param replicationPeriodMillis the replication period
     * @return this config
     */
    public CRDTReplicationConfig setReplicationPeriodMillis(int replicationPeriodMillis) {
        if (replicationPeriodMillis <= 0) {
            throw new InvalidConfigurationException("The value of replicationPeriodMillis must be a non-null positive integer");
        }
        this.replicationPeriodMillis = replicationPeriodMillis;
        return this;
    }

    /**
     * Returns the maximum number of target members that we replicate the CRDT
     * states to in one period.
     */
    public int getMaxConcurrentReplicationTargets() {
        return maxConcurrentReplicationTargets;
    }

    /**
     * Sets the maximum number of target members that we replicate the CRDT states
     * to in one period. A higher count will lead to states being disseminated
     * more rapidly at the expense of burst-like behaviour - one update to a
     * CRDT will lead to a sudden burst in the number of replication messages
     * in a short time interval.
     *
     * @param maxConcurrentReplicationTargets the maximum number of replication
     *                                        targets in a replication period
     * @return this config
     */
    public CRDTReplicationConfig setMaxConcurrentReplicationTargets(int maxConcurrentReplicationTargets) {
        if (maxConcurrentReplicationTargets <= 0) {
            throw new InvalidConfigurationException("The value of maxConcurrentReplicationTargets must be a non-null"
                    + " positive integer");
        }
        this.maxConcurrentReplicationTargets = maxConcurrentReplicationTargets;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CRDTReplicationConfig that = (CRDTReplicationConfig) o;
        return replicationPeriodMillis == that.replicationPeriodMillis
                && maxConcurrentReplicationTargets == that.maxConcurrentReplicationTargets;
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicationPeriodMillis, maxConcurrentReplicationTargets);
    }
}
