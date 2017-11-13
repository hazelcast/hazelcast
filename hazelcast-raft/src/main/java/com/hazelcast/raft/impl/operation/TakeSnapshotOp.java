package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

/**
 * {@code RaftOperation} to take snapshot using related {@link SnapshotAwareService#takeSnapshot(RaftGroupId, long)}.
 * <p>
 * This operation is executed locally, is not appended to Raft log and never serialized.
 */
public class TakeSnapshotOp extends RaftOperation {

    private final RaftGroupId groupId;

    public TakeSnapshotOp(String serviceName, RaftGroupId groupId) {
        this.groupId = groupId;
        setServiceName(serviceName);
    }

    @Override
    public Object doRun(long commitIndex) {
        SnapshotAwareService service = getService();
        return service.takeSnapshot(groupId, commitIndex);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
