package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.operation.GetLockCountOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.raft.service.lock.RaftLockService.SERVICE_NAME;

public class RaftLockProxy extends SessionAwareProxy implements ILock {

    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public RaftLockProxy(String name, RaftGroupId groupId, SessionManagerService sessionManager,
            RaftInvocationManager invocationManager) {
        super(sessionManager, groupId);
        this.name = name;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public void lock() {
        UUID invUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            RaftOp op = new LockOp(name, sessionId, ThreadUtil.getThreadId(), invUid);
            Future<Object> f = raftInvocationManager.invoke(groupId, op);
            try {
                join(f);
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLock(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        UUID invUid = UuidUtil.newUnsecureUUID();
        long timeoutMs = Math.max(0, unit.toMillis(time));
        for (;;) {
            long sessionId = acquireSession();
            RaftOp op = new TryLockOp(name, sessionId, ThreadUtil.getThreadId(), invUid, timeoutMs);
            Future<Long> f = raftInvocationManager.invoke(groupId, op);
            try {
                return join(f) > 0L;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void unlock() {
        final long sessionId = getSession();
        if (sessionId < 0) {
            throw new IllegalMonitorStateException();
        }
        UUID invUid = UuidUtil.newUnsecureUUID();
        Future f = raftInvocationManager.invoke(groupId, new UnlockOp(name, sessionId, ThreadUtil.getThreadId(), invUid));
        try {
            join(f);
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICondition newCondition(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        long sessionId = getSession();
        if (sessionId < 0) {
            return false;
        }

        Future<Integer> f = raftInvocationManager.invoke(groupId, new GetLockCountOp(name, sessionId, ThreadUtil.getThreadId()));
        return join(f) > 0;
    }

    @Override
    public int getLockCount() {
        Future<Integer> f = raftInvocationManager.invoke(groupId, new GetLockCountOp(name));
        return join(f);
    }

    @Override
    public long getRemainingLeaseTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Object getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void destroy() {
        join(raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)));
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }
}
