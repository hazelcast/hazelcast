package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.operation.ForceUnlockOp;
import com.hazelcast.raft.service.lock.operation.GetLockCountOp;
import com.hazelcast.raft.service.lock.operation.GetLockFenceOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.session.SessionManagerService;

import java.util.UUID;
import java.util.concurrent.Future;

public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    private final RaftInvocationManager invocationManager;

    public RaftFencedLockProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager, RaftGroupId groupId,
                               String name) {
        super(sessionManager, groupId, name);
        this.invocationManager = invocationManager;
    }

    @Override
    protected Future<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        return invoke(new LockOp(name, sessionId, threadId, invocationUid));
    }

    @Override
    protected Future<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid,
                                     long timeoutMillis) {
        return invoke(new TryLockOp(name, sessionId, threadId, invocationUid, timeoutMillis));
    }

    @Override
    protected Future<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        return invoke(new UnlockOp(name, sessionId, threadId, invocationUid));
    }

    @Override
    protected Future<Object> doForceUnlock(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        return invoke(new ForceUnlockOp(name, expectedFence, invocationUid));
    }

    @Override
    protected Future<Long> doGetLockFence(RaftGroupId groupId, String name) {
        return invoke(new GetLockFenceOp(name));
    }

    @Override
    protected Future<Integer> doGetLockCount(RaftGroupId groupId, String name) {
        return invoke(new GetLockCountOp(name));
    }

    private <T> Future<T> invoke(RaftOp op) {
        return invocationManager.invoke(groupId, op);
    }
}
