/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;

import java.util.ArrayList;
import java.util.List;

class CompositeInterceptor implements PartitionReplicaInterceptor {

    private List<PartitionReplicaInterceptor> interceptors;

    private CompositeInterceptor() {
    }

    public static CompositeInterceptor create() {
        return new CompositeInterceptor();
    }

    public CompositeInterceptor add(PartitionReplicaInterceptor interceptor) {
        if (interceptors == null) {
            interceptors = new ArrayList<>();
        }
        interceptors.add(interceptor);
        return this;
    }

    @Override
    public void replicaChanged(int partitionId, int replicaIndex,
                               PartitionReplica oldReplica, PartitionReplica newReplica) {
        if (interceptors.isEmpty()) {
            return;
        }

        for (int i = 0; i < interceptors.size(); i++) {
            interceptors.get(i).replicaChanged(partitionId, replicaIndex, oldReplica, newReplica);
        }
    }
}
