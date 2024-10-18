/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import com.hazelcast.internal.json.JsonObject;

import javax.annotation.Nonnull;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.UNKNOWN;

public class RuntimeContext {

    /** specified number of replicas, corresponds to {@code StatefulSetSpec.replicas} */
    private final int specifiedReplicaCount;

    /** number of ready replicas, corresponds to {@code StatefulSetStatus.readyReplicas} */
    private final int readyReplicas;

    /**
     * number of replicas created by the current revision of the StatefulSet,
     * corresponds to {@code StatefulSetStatus.currentReplicas}
     */
    private final int currentReplicas;

    private final String resourceVersion;

    public RuntimeContext(int specifiedReplicaCount, int readyReplicas, int currentReplicas,
                          String resourceVersion) {
        this.specifiedReplicaCount = specifiedReplicaCount;
        this.readyReplicas = readyReplicas;
        this.currentReplicas = currentReplicas;
        this.resourceVersion = resourceVersion;
    }

    @Nonnull
    public static RuntimeContext from(@Nonnull JsonObject statefulSet, String resourceVersion) {
        int specReplicas = statefulSet.get("spec").asObject().getInt("replicas", UNKNOWN);
        int readyReplicas = statefulSet.get("status").asObject().getInt("readyReplicas", UNKNOWN);
        int replicas = statefulSet.get("status").asObject().getInt("currentReplicas", UNKNOWN);
        return new RuntimeContext(specReplicas, readyReplicas, replicas, resourceVersion);
    }

    public int getSpecifiedReplicaCount() {
        return specifiedReplicaCount;
    }

    public int getReadyReplicas() {
        return readyReplicas;
    }

    public int getCurrentReplicas() {
        return currentReplicas;
    }

    public String getResourceVersion() {
        return resourceVersion;
    }

    @Override
    public String toString() {
        return "RuntimeContext{"
                + "specifiedReplicaCount=" + specifiedReplicaCount
                + ", readyReplicas=" + readyReplicas
                + ", currentReplicas=" + currentReplicas
                + ", resourceVersion='" + resourceVersion + '\''
                + '}';
    }
}
