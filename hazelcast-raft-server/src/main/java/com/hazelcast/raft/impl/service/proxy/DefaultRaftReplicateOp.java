package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

public class DefaultRaftReplicateOp extends RaftReplicateOp {

    private RaftOp raftOp;

    public DefaultRaftReplicateOp() {
    }

    public DefaultRaftReplicateOp(RaftGroupId groupId, RaftOp raftOp) {
        super(groupId);
        this.raftOp = raftOp;
    }

    @Override
    protected RaftOp getRaftOp() {
        return raftOp;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(raftOp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        raftOp = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", raftOp=").append(raftOp);
    }
}
