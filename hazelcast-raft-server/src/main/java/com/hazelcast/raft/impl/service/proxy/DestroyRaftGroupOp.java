package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

public class DestroyRaftGroupOp extends RaftReplicateOp {

    public DestroyRaftGroupOp() {
    }

    public DestroyRaftGroupOp(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        return raftNode.replicate(new DestroyRaftGroupCmd());
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
        return RaftServiceDataSerializerHook.DESTROY_RAFT_GROUP_OP;
    }
}
