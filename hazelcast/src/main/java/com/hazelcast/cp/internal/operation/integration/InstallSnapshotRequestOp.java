/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.operation.integration;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceSerializerConstants;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Carries a {@link InstallSnapshotRequest} RPC from a Raft group leader to a follower
 * <br>
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 */
public class InstallSnapshotRequestOp extends AsyncRaftOp {

    private InstallSnapshotRequest installSnapshotRequest;

    public InstallSnapshotRequestOp() {
    }

    public InstallSnapshotRequestOp(CPGroupId groupId, InstallSnapshotRequest installSnapshotRequest) {
        super(groupId);
        this.installSnapshotRequest = installSnapshotRequest;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handleSnapshotRequest(groupId, installSnapshotRequest, target);
    }

    @Override
    public int getClassId() {
        return RaftServiceSerializerConstants.INSTALL_SNAPSHOT_REQUEST_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(installSnapshotRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        installSnapshotRequest = in.readObject();
    }
}
