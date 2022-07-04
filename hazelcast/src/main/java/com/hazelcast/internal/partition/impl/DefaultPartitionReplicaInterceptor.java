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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;

/**
 * PartitionReplicaInterceptor used to intercept partition changes internally.
 * Most significant responsibility of this interceptor is to increment the partition-state version on each change
 * and cancel any ongoing replica synchronization on the changed partition.
 */
final class DefaultPartitionReplicaInterceptor implements PartitionReplicaInterceptor {
    private final InternalPartitionServiceImpl partitionService;

    DefaultPartitionReplicaInterceptor(InternalPartitionServiceImpl partitionService) {
        this.partitionService = partitionService;
    }

    @Override
    public void replicaChanged(int partitionId, int replicaIndex, PartitionReplica oldReplica, PartitionReplica newReplica) {
        if (replicaIndex == 0) {
            partitionService.getReplicaManager().cancelReplicaSync(partitionId);
        }
        partitionService.getPartitionStateManager().updateStamp();
    }
}
