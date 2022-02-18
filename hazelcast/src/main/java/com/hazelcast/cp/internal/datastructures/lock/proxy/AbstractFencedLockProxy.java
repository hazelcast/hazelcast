/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.datastructures.lock.proxy;

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.lock.exception.LockAcquireLimitReachedException;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.internal.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements proxy methods for Raft-based {@link FencedLock} API.
 * Lock reentrancy is implemented locally.
 */
public abstract class AbstractFencedLockProxy extends SessionAwareProxy implements FencedLock {

    protected final String proxyName;
    protected final String objectName;
    // thread id -> id of the session that has acquired the lock
    private final Map<Long, Long> lockedSessionIds = new ConcurrentHashMap<Long, Long>();

    public AbstractFencedLockProxy(AbstractProxySessionManager sessionManager, RaftGroupId groupId, String proxyName,
                                       String objectName) {
        super(sessionManager, groupId);
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    protected abstract InternalCompletableFuture<Long> doLock(long sessionId, long threadId, UUID invocationUid);

    protected abstract InternalCompletableFuture<Long> doTryLock(long sessionId, long threadId, UUID invocationUid,
                                                                 long timeoutMillis);

    protected abstract InternalCompletableFuture<Boolean> doUnlock(long sessionId, long threadId, UUID invocationUid);

    protected abstract InternalCompletableFuture<LockOwnershipState> doGetLockOwnershipState();

    @Override
    public void lock() {
        lockAndGetFence();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        for (;;) {
            long sessionId = acquireSession();
            verifyLockedSessionIdIfPresent(threadId, sessionId, true);

            try {
                long fence = doLock(sessionId, threadId, invocationUid).get();
                if (fence != INVALID_FENCE) {
                    lockedSessionIds.put(threadId, sessionId);
                    return;
                }

                throw new LockAcquireLimitReachedException("Lock[" + objectName + "] reentrant lock limit is already reached!");
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                verifyNoLockedSessionIdPresent(threadId);
            } catch (WaitKeyCancelledException e) {
                releaseSession(sessionId);
                throw new IllegalMonitorStateException("Lock[" + objectName + "] not acquired because the lock call "
                        + "on the CP group is cancelled, possibly because of another indeterminate call from the same thread.");
            } catch (Throwable t) {
                releaseSession(sessionId);
                if (t instanceof InterruptedException) {
                    throw (InterruptedException) t;
                } else {
                    throw rethrow(t);
                }
            }
        }
    }

    @Override
    public final long lockAndGetFence() {
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        for (;;) {
            long sessionId = acquireSession();
            verifyLockedSessionIdIfPresent(threadId, sessionId, true);

            try {
                long fence = doLock(sessionId, threadId, invocationUid).joinInternal();
                if (fence != INVALID_FENCE) {
                    lockedSessionIds.put(threadId, sessionId);
                    return fence;
                }

                throw new LockAcquireLimitReachedException("Lock[" + objectName + "] reentrant lock limit is already reached!");
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                verifyNoLockedSessionIdPresent(threadId);
            } catch (WaitKeyCancelledException e) {
                releaseSession(sessionId);
                throw new IllegalMonitorStateException("Lock[" + objectName + "] not acquired because the lock call "
                        + "on the CP group is cancelled, possibly because of another indeterminate call from the same thread.");
            } catch (Throwable t) {
                releaseSession(sessionId);
                throw rethrow(t);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLockAndGetFence() != INVALID_FENCE;
    }

    @Override
    public final long tryLockAndGetFence() {
        return tryLockAndGetFence(0, MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, @Nonnull TimeUnit unit) {
        return tryLockAndGetFence(time, unit) != INVALID_FENCE;
    }

    @Override
    public final long tryLockAndGetFence(long time, @Nonnull TimeUnit unit) {
        checkNotNull(unit);

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        long timeoutMillis = Math.max(0, unit.toMillis(time));
        long start;

        for (;;) {
            start = Clock.currentTimeMillis();
            long sessionId = acquireSession();
            verifyLockedSessionIdIfPresent(threadId, sessionId, true);

            try {
                long fence = doTryLock(sessionId, threadId, invocationUid, timeoutMillis).joinInternal();
                if (fence != INVALID_FENCE) {
                    lockedSessionIds.put(threadId, sessionId);
                } else {
                    releaseSession(sessionId);
                }

                return fence;
            } catch (WaitKeyCancelledException e) {
                releaseSession(sessionId);
                return INVALID_FENCE;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                verifyNoLockedSessionIdPresent(threadId);

                timeoutMillis -= (Clock.currentTimeMillis() - start);
                if (timeoutMillis <= 0) {
                    return INVALID_FENCE;
                }
            } catch (Throwable t) {
                releaseSession(sessionId);
                throw rethrow(t);
            }
        }
    }

    @Override
    @SuppressFBWarnings("IMSE_DONT_CATCH_IMSE")
    public final void unlock() {
        long threadId = getThreadId();
        long sessionId = getSession();

        // the order of the following checks is important.
        verifyLockedSessionIdIfPresent(threadId, sessionId, false);
        if (sessionId == NO_SESSION_ID) {
            lockedSessionIds.remove(threadId);
            throw newIllegalMonitorStateException();
        }

        try {
            boolean stillLockedByCurrentThread = doUnlock(sessionId, threadId, newUnsecureUUID()).joinInternal();
            if (stillLockedByCurrentThread) {
                lockedSessionIds.put(threadId, sessionId);
            } else {
                lockedSessionIds.remove(threadId);
            }

            releaseSession(sessionId);
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            lockedSessionIds.remove(threadId);

            throw newLockOwnershipLostException(sessionId);
        } catch (IllegalMonitorStateException e) {
            lockedSessionIds.remove(threadId);

            throw  e;
        }
    }

    @Override
    public final Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getFence() {
        long threadId = getThreadId();
        long sessionId = getSession();

        // the order of the following checks is important.
        verifyLockedSessionIdIfPresent(threadId, sessionId, false);
        if (sessionId == NO_SESSION_ID) {
            lockedSessionIds.remove(threadId);
            throw newIllegalMonitorStateException();
        }

        LockOwnershipState ownership = doGetLockOwnershipState().joinInternal();
        if (ownership.isLockedBy(sessionId, threadId)) {
            lockedSessionIds.put(threadId, sessionId);
            return ownership.getFence();
        }

        verifyNoLockedSessionIdPresent(threadId);
        throw newIllegalMonitorStateException();
    }

    @Override
    public final boolean isLocked() {
        long threadId = getThreadId();
        long sessionId = getSession();

        verifyLockedSessionIdIfPresent(threadId, sessionId, false);

        LockOwnershipState ownership = doGetLockOwnershipState().joinInternal();
        if (ownership.isLockedBy(sessionId, threadId)) {
            lockedSessionIds.put(threadId, sessionId);
            return true;
        }

        verifyNoLockedSessionIdPresent(threadId);

        return ownership.isLocked();
    }

    @Override
    public final boolean isLockedByCurrentThread() {
        long threadId = getThreadId();
        long sessionId = getSession();

        verifyLockedSessionIdIfPresent(threadId, sessionId, false);

        LockOwnershipState ownership = doGetLockOwnershipState().joinInternal();
        boolean lockedByCurrentThread = ownership.isLockedBy(sessionId, threadId);
        if (lockedByCurrentThread) {
            lockedSessionIds.put(threadId, sessionId);
        } else {
            verifyNoLockedSessionIdPresent(threadId);
        }

        return lockedByCurrentThread;
    }

    @Override
    public final int getLockCount() {
        long threadId = getThreadId();
        long sessionId = getSession();

        verifyLockedSessionIdIfPresent(threadId, sessionId, false);

        LockOwnershipState ownership = doGetLockOwnershipState().joinInternal();
        if (ownership.isLockedBy(sessionId, threadId)) {
            lockedSessionIds.put(threadId, sessionId);
        } else {
            verifyNoLockedSessionIdPresent(threadId);
        }

        return ownership.getLockCount();
    }

    @Override
    public void destroy() {
        lockedSessionIds.clear();
    }

    @Override
    public final String getName() {
        return proxyName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    // !!! only for testing !!!
    public Long getLockedSessionId(long threadId) {
        return lockedSessionIds.get(threadId);
    }

    private void verifyLockedSessionIdIfPresent(long threadId, long sessionId, boolean releaseSession) {
        Long lockedSessionId = lockedSessionIds.get(threadId);
        if (lockedSessionId != null && lockedSessionId != sessionId) {
            lockedSessionIds.remove(threadId);
            if (releaseSession) {
                releaseSession(sessionId);
            }

            throw newLockOwnershipLostException(lockedSessionId);
        }
    }

    private void verifyNoLockedSessionIdPresent(long threadId) {
        Long lockedSessionId = lockedSessionIds.remove(threadId);
        if (lockedSessionId != null) {
            lockedSessionIds.remove(threadId);
            throw newLockOwnershipLostException(lockedSessionId);
        }
    }

    private IllegalMonitorStateException newIllegalMonitorStateException() {
        return new IllegalMonitorStateException("Current thread is not owner of the Lock[" + proxyName + "]");
    }

    private LockOwnershipLostException newLockOwnershipLostException(long sessionId) {
        return new LockOwnershipLostException("Current thread is not owner of the Lock[" + proxyName + "] because its Session["
                + sessionId + "] is closed by server!");
    }

}
