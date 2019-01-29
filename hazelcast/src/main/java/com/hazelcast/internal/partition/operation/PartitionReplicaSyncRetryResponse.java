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

package com.hazelcast.internal.partition.operation;

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
import com.hazelcast.spi.ServiceNamespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * The response to a {@link PartitionReplicaSyncRequest} that the replica should retry. This will reset the current ongoing
 * synchronization request state and retry the request if this node is still a replica of this partition.
 */
// RU_COMPAT_39: Do not remove Versioned interface!
// Version info is needed on 3.9 members while deserializing the operation.
public class PartitionReplicaSyncRetryResponse
        extends AbstractPartitionOperation
        implements PartitionAwareOperation, BackupOperation, MigrationCycleOperation, Versioned {

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
        out.writeInt(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            out.writeObject(namespace);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        namespaces = new ArrayList<ServiceNamespace>(len);
        for (int i = 0; i < len; i++) {
            ServiceNamespace ns = in.readObject();
            namespaces.add(ns);
        }
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_RETRY_RESPONSE;
    }
}
