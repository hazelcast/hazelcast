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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.CPSubsystemImpl;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.Collection;

public class GetCPObjectInfosOp extends Operation implements IdentifiedDataSerializable, PartitionAwareOperation {

    private CPGroupId groupId;
    private String serviceName;
    private boolean returnTombstone;
    private Collection<String> result;

    public GetCPObjectInfosOp() {
    }

    public GetCPObjectInfosOp(CPGroupId groupId, String serviceName, boolean returnTombstone) {
        this.groupId = groupId;
        this.serviceName = serviceName;
        this.returnTombstone = returnTombstone;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getNodeEngine().getService(RaftService.SERVICE_NAME);
        RaftNode raftNode = service.getRaftNode(groupId);

        if (raftNode == null) {
            throw new CPSubsystemException("This member doesn't know group id " + groupId, null);
        }

        if (!raftNode.getLocalMember().equals(raftNode.getLeader())) {
            throw new NotLeaderException(groupId, raftNode.getLocalMember(), raftNode.getLeader());
        }

        CPSubsystemImpl cpSubsystem = (CPSubsystemImpl) getNodeEngine().getHazelcastInstance().getCPSubsystem();
        result = cpSubsystem.getCPObjectNames(groupId, serviceName, returnTombstone);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.GET_CP_OBJECT_INFOS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeString(serviceName);
        out.writeBoolean(returnTombstone);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        serviceName = in.readString();
        returnTombstone = in.readBoolean();
    }
}
