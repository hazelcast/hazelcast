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

package com.hazelcast.cp.internal.raftop.snapshot;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftOp;

import java.io.IOException;

/**
 * {@code RaftOp} to restore snapshot using related
 * {@link SnapshotAwareService#takeSnapshot(CPGroupId, long)}.
 * <p>
 * This operation is appended to Raft log in
 * {@link com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry} and sent to
 * followers via
 * {@link com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot} RPC.
 */
public class RestoreSnapshotOp extends RaftOp implements IdentifiedDataSerializable {

    private String serviceName;
    private Object snapshot;

    public RestoreSnapshotOp() {
    }

    public RestoreSnapshotOp(String serviceName, Object snapshot) {
        this.serviceName = serviceName;
        this.snapshot = snapshot;
    }

    public Object getSnapshot() {
        return snapshot;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        SnapshotAwareService service = getService();
        service.restoreSnapshot(groupId, commitIndex, snapshot);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(serviceName);
        out.writeObject(snapshot);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readString();
        snapshot = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.RESTORE_SNAPSHOT_OP;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", snapshot=").append(snapshot);
    }
}
