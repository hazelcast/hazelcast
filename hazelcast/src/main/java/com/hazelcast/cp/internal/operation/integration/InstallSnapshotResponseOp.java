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
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Sends an {@link InstallSnapshotResponse} RPC from a follower to the Raft group leader.
 * <br>
 * <br>
 * Sent by a follower in response to a snapshot installation request, this message allows the follower
 * to request additional snapshot chunks from the leader in any order to complete synchronization.
 */
public class InstallSnapshotResponseOp extends AsyncRaftOp {

    private InstallSnapshotResponse installSnapshotResponse;

    public InstallSnapshotResponseOp() {
    }

    public InstallSnapshotResponseOp(CPGroupId groupId, InstallSnapshotResponse installSnapshotResponse) {
        super(groupId);
        this.installSnapshotResponse = installSnapshotResponse;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handleSnapshotResponse(groupId, installSnapshotResponse, target);
    }

    @Override
    public int getClassId() {
        return RaftServiceSerializerConstants.INSTALL_SNAPSHOT_RESPONSE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(installSnapshotResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        installSnapshotResponse = in.readObject();
    }
}
