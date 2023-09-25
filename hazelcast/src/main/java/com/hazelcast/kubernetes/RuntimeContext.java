/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nullable;

public class RuntimeContext {

    /**
     * Unknown value
     */
    public static final long UNKNOWN = -1;

    // specified number of replicas. Corresponds to StatefulSetSpec.replicas
    private final int specifiedReplicaCount;

    // number of ready replicas. Corresponds to StatefulSetStatus.readyReplicas
    private final int readyReplicas;

    // number of replicas created by the current revision of the StatefulSet.
    // Populated from StatefulSetStatus.currentReplicas
    private final int currentReplicas;

    @Nullable
    private final String resourceVersion;

    public RuntimeContext(int specifiedReplicaCount, int readyReplicas, int currentReplicas,
                          @Nullable String resourceVersion) {
        this.specifiedReplicaCount = specifiedReplicaCount;
        this.readyReplicas = readyReplicas;
        this.currentReplicas = currentReplicas;
        this.resourceVersion = resourceVersion;
    }

    public int getSpecifiedReplicaCount() {
        return specifiedReplicaCount;
    }

    public int getReadyReplicas() {
        return readyReplicas;
    }

    public String getResourceVersion() {
        return resourceVersion;
    }

    public int getCurrentReplicas() {
        return currentReplicas;
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
