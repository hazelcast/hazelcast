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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author mdogan 4/11/13
 */
public final class CheckReplicaVersion extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

    private long version;
    private boolean returnResponse;

    private transient boolean response;

    public CheckReplicaVersion() {
    }

    public CheckReplicaVersion(long version, boolean returnResponse) {
        this.version = version;
        this.returnResponse = returnResponse;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final PartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();
        final long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
        final long currentVersion = currentVersions[replicaIndex];

        if (currentVersion != version) {
            getLogger().info("Backup partition version is not matching version of the owner " +
                    "-> " + currentVersion + " -vs- " + version);
            partitionService.syncPartitionReplica(partitionId, replicaIndex, false);
            response = false;
        } else {
            response = true;
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return returnResponse;
    }

    public Object getResponse() {
        return response;
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
        out.writeLong(version);
        out.writeBoolean(returnResponse);
    }

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
        sb.append('}');
        return sb.toString();
    }
}
