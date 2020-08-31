/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

/**
 * Sent from the master to check the partition table state stamp on target member.
 *
 * @since 3.12
 */
public final class PartitionStateCheckOperation extends AbstractPartitionOperation
        implements MigrationCycleOperation, Versioned {

    private long stamp;

    //RU_COMPAT_4_0
    @Deprecated
    private int version;

    private transient boolean stale;

    public PartitionStateCheckOperation() {
    }

    public PartitionStateCheckOperation(long stamp, @Deprecated int version) {
        this.stamp = stamp;
        this.version = version;
    }

    @Override
    public void run() {
        ILogger logger = getLogger();
        InternalPartitionServiceImpl partitionService = getService();

        if (getNodeEngine().getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V4_1)) {
            long currentStamp = partitionService.getPartitionStateStamp();
            if (currentStamp != stamp) {
                stale = true;
                if (logger.isFineEnabled()) {
                    logger.fine("Partition table is stale! Current stamp: " + currentStamp
                            + ", master stamp: " + stamp);
                }
            }
        } else {
            //RU_COMPAT_4_0
            int currentVersion = partitionService.getPartitionStateVersion();
            if (currentVersion < version) {
                stale = true;
                if (logger.isFineEnabled()) {
                    logger.fine("Partition table is stale! Current version: " + currentVersion
                            + ", master version: " + version);
                }
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
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            stamp = in.readLong();
        } else {
            version = in.readInt();
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeLong(stamp);
        } else {
            out.writeInt(version);
        }
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PARTITION_STATE_CHECK_OP;
    }
}
