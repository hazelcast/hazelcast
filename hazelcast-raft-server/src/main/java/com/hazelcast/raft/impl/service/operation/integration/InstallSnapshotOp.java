package com.hazelcast.raft.impl.service.operation.integration;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

public class InstallSnapshotOp extends AsyncRaftOp {

    private InstallSnapshot installSnapshot;

    public InstallSnapshotOp() {
    }

    public InstallSnapshotOp(RaftGroupId groupId, InstallSnapshot installSnapshot) {
        super(groupId);
        this.installSnapshot = installSnapshot;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleSnapshot(groupId, installSnapshot);
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(installSnapshot);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        installSnapshot = in.readObject();
    }
}
