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

package com.hazelcast.kubernetes;

import javax.annotation.Nullable;

public class RuntimeContext {

    /**
     * Unknown value
     */
    public static final long UNKNOWN = -1;

    // desired number of replicas. Corresponds to StatefulSetSpec.replicas
    private final int desiredNumberOfMembers;

    // number of ready replicas. Corresponds to StatefulSetStatus.readyReplicas
    private final int readyReplicas;

    @Nullable
    private final String resourceVersion;

    public RuntimeContext(int desiredNumberOfMembers, int readyReplicas,
                          @Nullable String resourceVersion) {
        this.desiredNumberOfMembers = desiredNumberOfMembers;
        this.readyReplicas = readyReplicas;
        this.resourceVersion = resourceVersion;
    }

    public int getDesiredNumberOfMembers() {
        return desiredNumberOfMembers;
    }

    public int getReadyReplicas() {
        return readyReplicas;
    }

    public String getResourceVersion() {
        return resourceVersion;
    }

    @Override
    public String toString() {
        return "RuntimeContext{"
                + "desiredNumberOfMembers=" + desiredNumberOfMembers
                + ", readyReplicas=" + readyReplicas
                + ", resourceVersion='" + resourceVersion
                + '\'' + '}';
    }
}
