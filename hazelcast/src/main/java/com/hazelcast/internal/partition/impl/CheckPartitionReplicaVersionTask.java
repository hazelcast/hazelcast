/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkNotNull;

// runs on the partition owner
final class CheckPartitionReplicaVersionTask extends AbstractPartitionPrimaryReplicaAntiEntropyTask {

    private final int replicaIndex;

    private final ExecutionCallback callback;

    CheckPartitionReplicaVersionTask(NodeEngineImpl nodeEngine, int partitionId, int replicaIndex, ExecutionCallback callback) {
        super(nodeEngine, partitionId);
        if (replicaIndex < 1 || replicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
        this.replicaIndex = replicaIndex;
        checkNotNull(callback);
        this.callback = callback;
    }

    @Override
    public void run() {
        InternalPartition partition = partitionService.getPartition(partitionId);
        if (!partition.isLocal() || partition.isMigrating()) {
            callback.onResponse(false);
            return;
        }

        Collection<ServiceNamespace> namespaces = retainAndGetNamespaces();
        Address target = partition.getReplicaAddress(replicaIndex);
        if (target == null) {
            callback.onResponse(false);
            return;
        }

        invokePartitionBackupReplicaAntiEntropyOp(replicaIndex, target, namespaces, callback);
    }

}
