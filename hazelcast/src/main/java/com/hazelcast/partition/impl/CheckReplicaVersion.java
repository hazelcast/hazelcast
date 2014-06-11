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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public final class CheckReplicaVersion extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

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
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        long currentVersion = currentVersions[replicaIndex - 1];

        if (currentVersion == version) {
            response = true;
        } else {
            logBackupVersionMismatch(currentVersion);
            partitionService.triggerPartitionReplicaSync(partitionId, replicaIndex);
            response = false;
        }
    }

    private void logBackupVersionMismatch(long currentVersion) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Partition: " + getPartitionId() + " version is not matching to version of the owner -> "
                    + currentVersion + " -vs- " + version);
        }
    }

    @Override
    public void afterRun() throws Exception {
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
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CheckReplicaVersion");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }
}
