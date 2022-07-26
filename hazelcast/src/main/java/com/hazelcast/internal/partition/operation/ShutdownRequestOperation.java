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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.UUID;

public class ShutdownRequestOperation
        extends AbstractPartitionOperation implements MigrationCycleOperation, Versioned {

    private UUID uuid;

    public ShutdownRequestOperation() {
    }

    public ShutdownRequestOperation(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        ILogger logger = getLogger();
        Address caller = getCallerAddress();

        if (partitionService.isLocalMemberMaster()) {
            ClusterService clusterService = getNodeEngine().getClusterService();
            Member member = clusterService.getMember(caller);
            if (member != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Received shutdown request from " + caller);
                }
                // RU_COMPAT 5.1
                 if (isClusterVersionLessThanV52() || member.getUuid().equals(uuid)) {
                    partitionService.onShutdownRequest(member);
                } else {
                    logger.warning("Ignoring shutdown request from " + uuid + " because it is not a member");
                }
            } else {
                logger.warning("Ignoring shutdown request from " + caller + " because it is not a member");
            }
        } else {
            logger.warning("Received shutdown request from " + caller + " but this node is not master.");
        }
    }

    // RU_COMPAT 5.1
    private boolean isClusterVersionLessThanV52() {
        return getNodeEngine().getClusterService()
                .getClusterVersion().isLessThan(Versions.V5_2);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.SHUTDOWN_REQUEST;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        // RU_COMPAT 5.1
        if (out.getVersion().isGreaterThan(Versions.V5_1)) {
            UUIDSerializationUtil.writeUUID(out, uuid);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // RU_COMPAT 5.1
        if (in.getVersion().isGreaterThan(Versions.V5_1)) {
            uuid = UUIDSerializationUtil.readUUID(in);
        }
    }
}
