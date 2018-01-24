package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
abstract class AbstractLockOp extends RaftOp {

    RaftGroupId groupId;
    String name;
    String uid;
    long threadId;
    UUID invUid;

    public AbstractLockOp() {
    }

    public AbstractLockOp(RaftGroupId groupId, String name, String uid, long threadId, UUID invUid) {
        this.groupId = groupId;
        this.name = name;
        this.uid = uid;
        this.threadId = threadId;
        this.invUid = invUid;
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeUTF(name);
        out.writeUTF(uid);
        out.writeLong(threadId);
        out.writeLong(invUid.getLeastSignificantBits());
        out.writeLong(invUid.getMostSignificantBits());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        name = in.readUTF();
        uid = in.readUTF();
        threadId = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invUid = new UUID(most, least);
    }
}
