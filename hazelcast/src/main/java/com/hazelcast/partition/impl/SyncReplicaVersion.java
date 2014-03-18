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
import com.hazelcast.partition.InternalPartitionService;
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

    public static final int OPERATION_TRY_COUNT = 10;
    public static final int OPERATION_TRY_PAUSE_MILLIS = 250;

    private final int syncReplicaIndex;
    private final Callback<Object> callback;
    private final boolean sync;

    public SyncReplicaVersion(int syncReplicaIndex, Callback<Object> callback) {
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
        this.syncReplicaIndex = syncReplicaIndex;
        this.callback = callback;
        this.sync = callback != null;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = syncReplicaIndex;
        InternalPartition partition = partitionService.getPartition(partitionId);
        Address target = partition.getReplicaAddress(replicaIndex);
        if (target == null) {
            notifyCallback(false);
            return;
        }

        invokeCheckReplicaVersion(partitionId, replicaIndex, target);
    }

    private void invokeCheckReplicaVersion(int partitionId, int replicaIndex, Address target) {
        InternalPartitionServiceImpl partitionService = getService();
        long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        long currentReplicaVersion = currentVersions[replicaIndex - 1];

        if (currentReplicaVersion > 0) {
            CheckReplicaVersion op = createCheckReplicaVersion(partitionId, replicaIndex, currentReplicaVersion);
            NodeEngine nodeEngine = getNodeEngine();
            OperationService operationService = nodeEngine.getOperationService();
            if (sync) {
                operationService.createInvocationBuilder(InternalPartitionService.SERVICE_NAME, op, target)
                        .setCallback(callback)
                        .setTryCount(OPERATION_TRY_COUNT)
                        .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                        .invoke();
            } else {
                operationService.send(op, target);
            }
        } else {
            notifyCallback(true);
        }
    }

    private void notifyCallback(boolean result) {
        if (callback != null) {
            callback.notify(result);
        }
    }

    private CheckReplicaVersion createCheckReplicaVersion(int partitionId, int replicaIndex, long currentVersion) {
        CheckReplicaVersion op = new CheckReplicaVersion(currentVersion, sync);
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(InternalPartitionService.SERVICE_NAME);
        return op;
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
        return InternalPartitionService.SERVICE_NAME;
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
