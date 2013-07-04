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

package com.hazelcast.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.*;

import java.io.IOException;

/**
 * @author mdogan 4/11/13
 */
// runs locally
final class SyncReplicaVersion extends Operation implements PartitionAwareOperation {

    private final int syncReplicaIndex;
    private final Callback<Object> callback;
    private final boolean sync;

    public SyncReplicaVersion(int syncReplicaIndex, Callback<Object> callback) {
        this.syncReplicaIndex = syncReplicaIndex;
        this.callback = callback;
        this.sync = callback != null;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final PartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = syncReplicaIndex;
        final PartitionImpl partition = partitionService.getPartitionView(partitionId);
        final Address target = partition.getReplicaAddress(replicaIndex);
        if (target != null) {
            final long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
            final NodeEngine nodeEngine = getNodeEngine();
            CheckReplicaVersion op = new CheckReplicaVersion(currentVersions[replicaIndex], sync);
            op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(PartitionServiceImpl.SERVICE_NAME);
            OperationService operationService = nodeEngine.getOperationService();
            if (sync) {
                operationService.createInvocationBuilder(PartitionServiceImpl.SERVICE_NAME, op, target)
                        .setCallback(callback).setTryCount(10).setTryPauseMillis(250).build().invoke();
            } else {
                operationService.send(op, target);
            }
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    public boolean validatesTarget() {
        return false;
    }

    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SyncReplicaVersion{");
        sb.append("partitionId=").append(getPartitionId());
        sb.append(", replicaIndex=").append(syncReplicaIndex);
        sb.append('}');
        return sb.toString();
    }
}
