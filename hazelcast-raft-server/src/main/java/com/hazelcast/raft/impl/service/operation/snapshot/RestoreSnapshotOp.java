package com.hazelcast.raft.impl.service.operation.snapshot;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.RaftOp;

import java.io.IOException;

/**
 * {@code RaftOp} to restore snapshot using related {@link SnapshotAwareService#takeSnapshot(RaftGroupId, long)}.
 * <p>
 * This operation is appended to Raft log in {@link com.hazelcast.raft.impl.log.SnapshotEntry} and send to followers
 * via {@link com.hazelcast.raft.impl.dto.InstallSnapshot} RPC.
 */
public class RestoreSnapshotOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private long commitIndex;
    private Object snapshot;

    public RestoreSnapshotOp() {
    }

    public RestoreSnapshotOp(String serviceName, RaftGroupId groupId, long commitIndex, Object snapshot) {
        this.groupId = groupId;
        setServiceName(serviceName);
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
    }

    @Override
    public Object doRun(long commitIndex) {
        assert this.commitIndex == commitIndex :
                " expected restore commit index: " + this.commitIndex + " given commit index: " + commitIndex;

        SnapshotAwareService service = getService();
        service.restoreSnapshot(groupId, commitIndex, snapshot);
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeLong(commitIndex);
        out.writeObject(snapshot);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        commitIndex = in.readLong();
        snapshot = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.RESTORE_SNAPSHOT_OP;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotOp{" + "groupId='" + groupId + '\'' + ", commitIndex=" + commitIndex + ", snapshot="
                + snapshot + '}';
    }
}
