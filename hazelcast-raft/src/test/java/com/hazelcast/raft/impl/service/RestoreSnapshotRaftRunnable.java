package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.testing.RaftRunnable;

import static com.hazelcast.util.Preconditions.checkTrue;

public class RestoreSnapshotRaftRunnable implements RaftRunnable {

    private RaftGroupId groupId;
    private long commitIndex;
    private Object snapshot;

    public RestoreSnapshotRaftRunnable(RaftGroupId groupId, long commitIndex, Object snapshot) {
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
