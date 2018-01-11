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

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

final class PartitionPrimaryReplicaAntiEntropyTask extends AbstractPartitionPrimaryReplicaAntiEntropyTask {

    PartitionPrimaryReplicaAntiEntropyTask(NodeEngineImpl nodeEngine, int partitionId) {
        super(nodeEngine, partitionId);
    }

    @Override
    public void run() {
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        if (!partition.isLocal() || partition.isMigrating()) {
            return;
        }

        Collection<ServiceNamespace> namespaces = retainAndGetNamespaces();

        for (int index = 1; index < MAX_REPLICA_COUNT; index++) {
            Address replicaAddress = partition.getReplicaAddress(index);
            if (replicaAddress != null) {
                invokePartitionBackupReplicaAntiEntropyOp(index, replicaAddress, namespaces, null);
            }
        }
    }

}
