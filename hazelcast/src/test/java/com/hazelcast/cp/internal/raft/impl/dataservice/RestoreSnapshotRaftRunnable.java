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

package com.hazelcast.cp.internal.raft.impl.dataservice;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.testing.RaftRunnable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

public class RestoreSnapshotRaftRunnable implements RaftRunnable, DataSerializable {

    private CPGroupId groupId;
    private long commitIndex;
    private Object snapshot;

    public RestoreSnapshotRaftRunnable() {
    }

    public RestoreSnapshotRaftRunnable(CPGroupId groupId, long commitIndex, Object snapshot) {
        this.groupId = groupId;
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Object getSnapshot() {
        return snapshot;
    }

    @Override
    public Object run(Object service, long commitIndex) {
        checkTrue(commitIndex == this.commitIndex, "snapshot commit indices are different! given: "
                + commitIndex + " expected: " + this.commitIndex);
        ((SnapshotAwareService<Object>) service).restoreSnapshot(groupId, commitIndex, snapshot);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeLong(commitIndex);
        out.writeObject(snapshot);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        commitIndex = in.readLong();
        snapshot = in.readObject();
    }

}
