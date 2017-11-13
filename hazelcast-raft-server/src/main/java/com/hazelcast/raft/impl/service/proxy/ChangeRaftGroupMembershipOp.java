package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

public class ChangeRaftGroupMembershipOp extends RaftReplicateOp {

    public static final int NAN_MEMBERS_COMMIT_INDEX = -1;

    private long membersCommitIndex;
    private RaftEndpoint endpoint;
    private MembershipChangeType changeType;

    public ChangeRaftGroupMembershipOp() {
    }

    public ChangeRaftGroupMembershipOp(RaftGroupId groupId, long membersCommitIndex, RaftEndpoint endpoint,
                                       MembershipChangeType changeType) {
        super(groupId);
        this.membersCommitIndex = membersCommitIndex;
        this.endpoint = endpoint;
        this.changeType = changeType;
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        if (membersCommitIndex == NAN_MEMBERS_COMMIT_INDEX) {
            return raftNode.replicateMembershipChange(endpoint, changeType);
        } else {
            return raftNode.replicateMembershipChange(endpoint, changeType, membersCommitIndex);
        }
    }

    @Override
    protected RaftOperation getRaftOperation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(membersCommitIndex);
        out.writeObject(endpoint);
        out.writeUTF(changeType.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        membersCommitIndex = in.readLong();
        endpoint = in.readObject();
        changeType = MembershipChangeType.valueOf(in.readUTF());
    }
}
