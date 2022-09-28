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

package com.hazelcast.instance.impl;

import java.util.Objects;

public class ClusterTopologyState {

    public static final ClusterTopologyState UNKNOWN = of(ClusterTopologyIntent.UNKNOWN, ClusterTopologyIntentTracker.UNKNOWN);
    public static final ClusterTopologyState NOT_IN_MANAGED_CONTEXT =
            of(ClusterTopologyIntent.NOT_IN_MANAGED_CONTEXT, ClusterTopologyIntentTracker.UNKNOWN);
    public static final ClusterTopologyState CLUSTER_SHUTDOWN =
            of(ClusterTopologyIntent.CLUSTER_SHUTDOWN, 0);
    public static final ClusterTopologyState CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS =
            of(ClusterTopologyIntent.CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS, 0);

    private final ClusterTopologyIntent clusterTopologyIntent;
    private final int desiredNumberOfReplicas;

    ClusterTopologyState(ClusterTopologyIntent clusterTopologyIntent, int desiredNumberOfReplicas) {
        this.clusterTopologyIntent = clusterTopologyIntent;
        this.desiredNumberOfReplicas = desiredNumberOfReplicas;
    }

    public ClusterTopologyIntent getClusterTopologyIntent() {
        return clusterTopologyIntent;
    }

    public int getDesiredNumberOfReplicas() {
        return desiredNumberOfReplicas;
    }

    public boolean hasIntent(ClusterTopologyIntent clusterTopologyIntent) {
        return this.clusterTopologyIntent == clusterTopologyIntent;
    }

    @Override
    public String toString() {
        return "ClusterTopologyState{"
                + "clusterTopologyIntent=" + clusterTopologyIntent
                + ", desiredNumberOfReplicas=" + desiredNumberOfReplicas
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterTopologyState)) {
            return false;
        }
        ClusterTopologyState that = (ClusterTopologyState) o;
        return desiredNumberOfReplicas == that.desiredNumberOfReplicas && clusterTopologyIntent == that.clusterTopologyIntent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterTopologyIntent, desiredNumberOfReplicas);
    }

    public static ClusterTopologyState of(ClusterTopologyIntent clusterTopologyIntent) {
        switch (clusterTopologyIntent) {
            case UNKNOWN:
                return UNKNOWN;
            case NOT_IN_MANAGED_CONTEXT:
                return NOT_IN_MANAGED_CONTEXT;
            case CLUSTER_SHUTDOWN:
                return CLUSTER_SHUTDOWN;
            case CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS:
                return CLUSTER_SHUTDOWN_WITH_MISSING_MEMBERS;
            default:
                return new ClusterTopologyState(clusterTopologyIntent, ClusterTopologyIntentTracker.UNKNOWN);
        }
    }

    public static ClusterTopologyState of(ClusterTopologyIntent clusterTopologyIntent, int desiredNumberOfReplicas) {
        return new ClusterTopologyState(clusterTopologyIntent, desiredNumberOfReplicas);
    }
}
