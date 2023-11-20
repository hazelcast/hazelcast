/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.impl.operations.DemoteDataMemberOp;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

/**
 * Request that the given member should no longer own any replica. The operation returns <code>true</code> if the
 * request was accepted and any needed migration has been scheduled.
 * After needed migrations have finished and the member no longer owns any replica, the member is notified with a
 * {@link DemoteResponseOperation}.
 *
 * @see DemoteDataMemberOp
 *
 * @since 5.4
 */
public class DemoteRequestOperation
        extends AbstractPartitionOperation implements MigrationCycleOperation {

    private UUID uuid;
    private transient boolean response;

    public DemoteRequestOperation() {
    }

    public DemoteRequestOperation(UUID uuid) {
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
                    logger.finest("Received demote request from " + caller);
                }
                if (member.getUuid().equals(uuid)) {
                    response = partitionService.onDemoteRequest(member);
                } else {
                    logger.warning("Ignoring demote request from " + uuid + " because it is not a member");
                    response = false;
                }
            } else {
                logger.warning("Ignoring demote request from " + caller + " because it is not a member");
                response = false;
            }
        } else {
            logger.warning("Received demote request from " + caller + " but this node is not master.");
            response = false;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.DEMOTE_REQUEST;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        UUIDSerializationUtil.writeUUID(out, uuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        uuid = UUIDSerializationUtil.readUUID(in);
    }
}
