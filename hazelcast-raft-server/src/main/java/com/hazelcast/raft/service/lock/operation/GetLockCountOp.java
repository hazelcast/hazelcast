package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetLockCountOp extends RaftOp {

    RaftGroupId groupId;
    String name;
    String uid;
    long threadId;

    public GetLockCountOp() {
    }

    public GetLockCountOp(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    public GetLockCountOp(RaftGroupId groupId, String name, String uid, long threadId) {
        this.groupId = groupId;
        this.name = name;
        this.uid = uid;
        this.threadId = threadId;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftLockService service = getService();
        Tuple2<LockEndpoint, Integer> result = service.lockCount(groupId, name);

        if (uid != null) {
            LockEndpoint endpoint = new LockEndpoint(uid, threadId);
            return endpoint.equals(result.element1) ? result.element2 : 0;
        }
        return result.element2;
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        name = in.readUTF();
        uid = in.readUTF();
        threadId = in.readLong();
    }
}
