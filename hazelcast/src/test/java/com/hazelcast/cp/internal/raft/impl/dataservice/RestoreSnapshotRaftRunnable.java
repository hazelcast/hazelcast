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

package com.hazelcast.cp.internal.raft.impl.dataservice;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.testing.RaftRunnable;

import static com.hazelcast.util.Preconditions.checkTrue;

public class RestoreSnapshotRaftRunnable implements RaftRunnable {

    private CPGroupId groupId;
    private long commitIndex;
    private Object snapshot;

    public RestoreSnapshotRaftRunnable(CPGroupId groupId, long commitIndex, Object snapshot) {
        this.groupId = groupId;
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
    }

    @Override
    public Object run(Object service, long commitIndex) {
        checkTrue(commitIndex == this.commitIndex, "snapshot commit indices are different! given: "
                + commitIndex + " expected: " + this.commitIndex);
        ((SnapshotAwareService<Object>) service).restoreSnapshot(groupId, commitIndex, snapshot);
        return null;
    }

}
