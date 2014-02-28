/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;

// runs locally
final class SyncReplicaVersion extends Operation implements PartitionAwareOperation, UrgentSystemOperation {

    private final int syncReplicaIndex;
    private final Callback<Object> callback;
    private final boolean sync;

    public SyncReplicaVersion(int syncReplicaIndex, Callback<Object> callback) {
        this.syncReplicaIndex = syncReplicaIndex;
        this.callback = callback;
        this.sync = callback != null;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        PartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = syncReplicaIndex;
        InternalPartition partition = partitionService.getPartition(partitionId);
        Address target = partition.getReplicaAddress(replicaIndex);
        if (target == null) {
            return;
        }

        long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        NodeEngine nodeEngine = getNodeEngine();
        CheckReplicaVersion op = new CheckReplicaVersion(currentVersions[replicaIndex], sync);
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(PartitionService.SERVICE_NAME);
        OperationService operationService = nodeEngine.getOperationService();
        if (sync) {
            operationService.createInvocationBuilder(PartitionService.SERVICE_NAME, op, target)
                    .setCallback(callback).setTryCount(10).setTryPauseMillis(250).invoke();
        } else {
            operationService.send(op, target);
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return PartitionService.SERVICE_NAME;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SyncReplicaVersion{");
        sb.append("partitionId=").append(getPartitionId());
        sb.append(", replicaIndex=").append(syncReplicaIndex);
        sb.append('}');
        return sb.toString();
    }
}
