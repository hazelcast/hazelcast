package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.command.TerminateRaftGroupCmd;

import java.io.IOException;

public class TerminateRaftGroupOp extends RaftReplicateOp {

    public TerminateRaftGroupOp() {
    }

    public TerminateRaftGroupOp(RaftGroupId raftGroupId) {
        super(raftGroupId);
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        return raftNode.replicate(new TerminateRaftGroupCmd());
    }

    @Override
    protected RaftOp getRaftOp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.TERMINATE_RAFT_GROUP_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }
}
