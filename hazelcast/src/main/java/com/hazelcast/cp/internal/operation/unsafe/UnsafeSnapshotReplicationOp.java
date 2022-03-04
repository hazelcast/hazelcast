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

package com.hazelcast.cp.internal.operation.unsafe;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMap;

/**
 * Partition-based replication operation for Raft services to be executed in UNSAFE mode.
 * This operation contains snapshots of Raft groups which belong to the same partition
 * and restores them on another partition replica.
 */
public class UnsafeSnapshotReplicationOp extends Operation implements IdentifiedDataSerializable {

    private Map<CPGroupId, Object> snapshots;

    public UnsafeSnapshotReplicationOp() {
    }

    public UnsafeSnapshotReplicationOp(Map<CPGroupId, Object> snapshots) {
        this.snapshots = snapshots;
    }

    @Override
    public void run() throws Exception {
        SnapshotAwareService service = getService();
        for (Entry<CPGroupId, Object> entry : snapshots.entrySet()) {
            CPGroupId groupId = entry.getKey();
            service.restoreSnapshot(groupId, 0, entry.getValue());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        writeMap(snapshots, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        snapshots = readMap(in);
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_SNAPSHOT_REPLICATE_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", snapshots=").append(snapshots);
    }
}
