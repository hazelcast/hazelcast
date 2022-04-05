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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;

/**
 * Represents ownership state of a Raft Lock
 */
public class LockOwnershipState implements IdentifiedDataSerializable {

    static final LockOwnershipState NOT_LOCKED = new LockOwnershipState(INVALID_FENCE, 0, -1, -1);

    private long fence;

    private int lockCount;

    private long sessionId;

    private long threadId;

    public LockOwnershipState() {
    }

    public LockOwnershipState(long fence, int lockCount, long sessionId, long threadId) {
        this.fence = fence;
        this.lockCount = lockCount;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public boolean isLocked() {
        return fence != INVALID_FENCE;
    }

    public boolean isLockedBy(long sessionId, long threadId) {
        return isLocked() && this.sessionId == sessionId && this.threadId == threadId;
    }

    /**
     * Returns fencing token of the lock if it is currently hold by some endpoint. {@link FencedLock#INVALID_FENCE} otherwise
     */
    public long getFence() {
        return fence;
    }

    public int getLockCount() {
        return lockCount;
    }

    /**
     * Returns the session id that currently holds the lock. Returns {@link AbstractProxySessionManager#NO_SESSION_ID} if not held
     */
    public long getSessionId() {
        return sessionId;
    }

    /**
     * Returns the thread id that holds the lock. If the lock is not held, return value is meaningless.
     * When the lock is held, pair of {@code &lt;session id, thread id&gt} is unique.
     */
    public long getThreadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.RAFT_LOCK_OWNERSHIP_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(fence);
        out.writeInt(lockCount);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fence = in.readLong();
        lockCount = in.readInt();
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    public String toString() {
        return "LockOwnershipState{" + "fence=" + fence + ", lockCount=" + lockCount + ", sessionId=" + sessionId
                + ", threadId=" + threadId + '}';
    }
}
