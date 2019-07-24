/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Sent from the master to check the partition table state version on target member.
 *
 * @since 3.12
 */
public final class PartitionStateVersionCheckOperation extends AbstractPartitionOperation
        implements MigrationCycleOperation {

    private int version;
    private transient boolean stale;

    public PartitionStateVersionCheckOperation() {
    }

    public PartitionStateVersionCheckOperation(int version) {
        this.version = version;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        int currentVersion = partitionService.getPartitionStateVersion();

        if (currentVersion < version) {
            stale = true;

            ILogger logger = getLogger();
            if (logger.isFineEnabled()) {
                logger.fine("Partition table is stale! Current version: " + currentVersion
                        + ", master version: " + version);
            }
        }
    }

    @Override
    public Object getResponse() {
        return !stale;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(version);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PARTITION_STATE_VERSION_CHECK_OP;
    }
}
