package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class LockOp extends AbstractLockOp {

    public LockOp() {
    }

    public LockOp(RaftGroupId groupId, String name, String uid, long threadId, UUID invUid) {
        super(groupId, name, uid, threadId, invUid);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = new LockEndpoint(uid, threadId);
        if (service.acquire(groupId, name, endpoint, commitIndex, invUid, true)) {
            return true;
        }
        return PostponedResponse.INSTANCE;
    }
}
