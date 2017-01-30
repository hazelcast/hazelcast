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

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

// should not be an urgent operation. required to be in order with backup operations on target node
public final class CheckReplicaVersion extends AbstractPartitionOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private long version;
    private boolean returnResponse;
    private boolean response;

    public CheckReplicaVersion() {
    }

    public CheckReplicaVersion(long version, boolean returnResponse) {
        this.version = version;
        this.returnResponse = returnResponse;
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        long[] currentVersions = replicaManager.getPartitionReplicaVersions(partitionId);
        long currentVersion = currentVersions[replicaIndex - 1];

        if (replicaManager.isPartitionReplicaVersionDirty(partitionId) || currentVersion != version) {
            logBackupVersionMismatch(currentVersion);
            replicaManager.triggerPartitionReplicaSync(partitionId, replicaIndex, 0L);
            response = false;
        } else {
            response = true;
        }
    }

    private void logBackupVersionMismatch(long currentVersion) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("partitionId=" + getPartitionId() + ", replicaIndex=" + getReplicaIndex()
                    + " version is not matching to version of the owner! "
                    + " expected-version=" + version + ", current-version=" + currentVersion);
        }
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    @Override
    public Object getResponse() {
        return response;
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
        out.writeLong(version);
        out.writeBoolean(returnResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        version = in.readLong();
        returnResponse = in.readBoolean();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", version=").append(version);
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.CHECK_REPLICA_VERSION;
    }
}
