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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * If the CP subsystem discovery process is completed, new Hazelcast nodes
 * skip the discovery step.
 * <p>
 * Please note that this operation is not a {@link RaftOp},
 * so it is not handled via the Raft layer.
 */
public class RaftServicePreJoinOp extends Operation implements IdentifiedDataSerializable {

    private boolean discoveryCompleted;

    private RaftGroupId metadataGroupId;

    public RaftServicePreJoinOp() {
    }

    public RaftServicePreJoinOp(boolean discoveryCompleted, RaftGroupId metadataGroupId) {
        this.discoveryCompleted = discoveryCompleted;
        this.metadataGroupId = metadataGroupId;
    }

    @Override
    public void run() {
        RaftService service = getService();
        MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
        metadataGroupManager.handleMetadataGroupId(metadataGroupId);
        if (discoveryCompleted) {
            metadataGroupManager.disableDiscovery();
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.RAFT_PRE_JOIN_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(discoveryCompleted);
        out.writeObject(metadataGroupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        discoveryCompleted = in.readBoolean();
        metadataGroupId = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", discoveryCompleted=").append(discoveryCompleted).append(", metadataGroupId=").append(metadataGroupId);
    }
}
