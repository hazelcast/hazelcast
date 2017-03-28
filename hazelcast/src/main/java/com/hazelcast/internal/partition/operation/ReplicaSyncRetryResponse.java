/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.cluster.impl.Versions;
import com.hazelcast.internal.partition.DefaultReplicaFragmentNamespace;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReplicaFragmentAwareService.ReplicaFragmentNamespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * The response to a {@link ReplicaSyncRequest} that the replica should retry. This will reset the current ongoing
 * synchronization request state and retry the request if this node is still a replica of this partition.
 */
public class ReplicaSyncRetryResponse extends AbstractPartitionOperation
        implements PartitionAwareOperation, BackupOperation, MigrationCycleOperation, Versioned {

    private Collection<ReplicaFragmentNamespace> namespaces;

    public ReplicaSyncRetryResponse() {
        namespaces = Collections.emptySet();
    }

    public ReplicaSyncRetryResponse(Collection<ReplicaFragmentNamespace> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        if (namespaces.isEmpty()) {
            // version 3.8
            replicaManager.clearReplicaSyncRequest(partitionId, DefaultReplicaFragmentNamespace.INSTANCE, replicaIndex);
        } else {
            for (ReplicaFragmentNamespace namespace : namespaces) {
                replicaManager.clearReplicaSyncRequest(partitionId, namespace, replicaIndex);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (out.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            out.writeInt(namespaces.size());
            for (ReplicaFragmentNamespace namespace : namespaces) {
                out.writeObject(namespace);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            int len = in.readInt();
            namespaces = new ArrayList<ReplicaFragmentNamespace>(len);
            for (int i = 0; i < len; i++) {
                ReplicaFragmentNamespace ns = in.readObject();
                namespaces.add(ns);
            }
        }
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_RETRY_RESPONSE;
    }
}
