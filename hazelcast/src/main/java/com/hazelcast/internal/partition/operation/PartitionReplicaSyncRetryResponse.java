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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.internal.services.ServiceNamespace;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * The response to a {@link PartitionReplicaSyncRequest} that the replica should retry. This will reset the current ongoing
 * synchronization request state and retry the request if this node is still a replica of this partition.
 */
public class PartitionReplicaSyncRetryResponse
        extends AbstractPartitionOperation
        implements PartitionAwareOperation, BackupOperation, MigrationCycleOperation {

    private Collection<ServiceNamespace> namespaces;

    public PartitionReplicaSyncRetryResponse() {
        namespaces = Collections.emptySet();
    }

    public PartitionReplicaSyncRetryResponse(Collection<ServiceNamespace> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        for (ServiceNamespace namespace : namespaces) {
            replicaManager.clearReplicaSyncRequest(partitionId, namespace, replicaIndex);
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
        writeCollection(namespaces, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespaces = readCollection(in);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_RETRY_RESPONSE;
    }
}
