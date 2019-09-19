/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState.NOT_LOCKED;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.min;

/**
 * State-machine implementation of the Raft-based lock
 */
public class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    /**
     * Max number of reentrant lock acquires
     */
    private int lockCountLimit;

    /**
     * Current owner of the lock
     */
    private LockInvocationKey owner;

    /**
     * Number of acquires the current lock owner has committed
     */
    private int lockCount;

    /**
     * Uids of the current lock owner's lock() / unlock() invocations,
     * and uid of the previous owner's last unlock() invocation.
     * Used for preventing duplicate execution of lock() / unlock() invocations
     */
    private Map<BiTuple<LockEndpoint, UUID>, RaftLockOwnershipState> ownerInvocationRefUids = new HashMap<>();

    RaftLock() {
    }

    RaftLock(CPGroupId groupId, String name, int lockCountLimit) {
        super(groupId, name);
        this.lockCountLimit = lockCountLimit > 0 ? lockCountLimit : Integer.MAX_VALUE;
    }

    /**
     * Assigns the lock to the endpoint, if the lock is not held. Lock count is
     * incremented if the endpoint already holds the lock. If some other
     * endpoint holds the lock and the second argument is true, a wait key is
     * created and added to the wait queue. Lock count is not incremented if
     * the lock request is a retry of the lock holder. If the lock request is
     * a retry of a lock endpoint that resides in the wait queue with the same
     * invocation uid, a retry wait key wait key is attached to the original
     * wait key. If the lock request is a new request of a lock endpoint that
     * resides in the wait queue with a different invocation uid, the existing
     * wait key is cancelled because it means the caller has stopped waiting
     * for response of the previous invocation. If the invocation uid is same
     * with one of the previous invocations of the current lock owner,
     * memorized result of the previous invocation is returned.
     */
    AcquireResult acquire(LockInvocationKey key, boolean wait) {
        LockEndpoint endpoint = key.endpoint();
        UUID invocationUid = key.invocationUid();
        RaftLockOwnershipState memorized = ownerInvocationRefUids.get(BiTuple.of(endpoint, invocationUid));
        if (memorized != null) {
            AcquireStatus status = memorized.isLocked() ? SUCCESSFUL : FAILED;
            return new AcquireResult(status, memorized.getFence(), Collections.emptyList());
        }

        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            if (lockCount == lockCountLimit) {
                ownerInvocationRefUids.put(BiTuple.of(endpoint, invocationUid), NOT_LOCKED);
                return AcquireResult.failed(Collections.emptyList());
            }

            lockCount++;
            ownerInvocationRefUids.put(BiTuple.of(endpoint, invocationUid), lockOwnershipState());
            return AcquireResult.acquired(owner.commitIndex());
        }

        // we must cancel waits keys of previous invocation of the endpoint
        // before adding a new wait key or even if we will not wait
        Collection<LockInvocationKey> cancelledWaitKeys = cancelWaitKeys(endpoint, invocationUid);

        if (wait) {
            addWaitKey(endpoint, key);
            return AcquireResult.waitKeyAdded(cancelledWaitKeys);
        }

        return AcquireResult.failed(cancelledWaitKeys);
    }

    private Collection<LockInvocationKey> cancelWaitKeys(LockEndpoint endpoint, UUID invocationUid) {
        Collection<LockInvocationKey> cancelled = null;
        WaitKeyContainer<LockInvocationKey> container = getWaitKeyContainer(endpoint);
        if (container != null && container.key().isDifferentInvocationOf(endpoint, invocationUid)) {
            cancelled = container.keyAndRetries();
            removeWaitKey(endpoint);
        }

        return cancelled != null ? cancelled : Collections.emptyList();
    }

    /**
     * Releases the lock. The lock is freed when lock count reaches to 0.
     * If the remaining lock count > 0 after a successful release, the lock is
     * still held by the endpoint. The lock is not released if it is a retry of
     * a previous successful release request of the current lock holder. If
     * the lock is assigned to some other endpoint after this release, wait
     * keys of the new lock holder are returned. If the release request fails
     * because the requesting endpoint does not hold the lock, all wait keys
     * of the endpoint are cancelled because that endpoint has stopped waiting
     * for response of its previous lock() invocation.
     */
    ReleaseResult release(LockEndpoint endpoint, UUID invocationUid) {
        return doRelease(endpoint, invocationUid, 1);
    }

    private ReleaseResult doRelease(LockEndpoint endpoint, UUID invocationUid, int releaseCount) {
        RaftLockOwnershipState memorized = ownerInvocationRefUids.get(BiTuple.of(endpoint, invocationUid));
        if (memorized != null) {
            return ReleaseResult.successful(memorized);
        }

        if (owner == null || !owner.endpoint().equals(endpoint)) {
            return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
        }

        lockCount = lockCount - min(lockCount, releaseCount);
        if (lockCount > 0) {
            RaftLockOwnershipState ownership = lockOwnershipState();
            ownerInvocationRefUids.put(BiTuple.of(endpoint, invocationUid), ownership);
            return ReleaseResult.successful(ownership);
        }

        removeInvocationRefUids(endpoint);

        Collection<LockInvocationKey> newOwnerWaitKeys = setNewLockOwner();

        ownerInvocationRefUids.put(BiTuple.of(endpoint, invocationUid), lockOwnershipState());

        return ReleaseResult.successful(lockOwnershipState(), newOwnerWaitKeys);
    }

    private void removeInvocationRefUids(LockEndpoint endpoint) {
        ownerInvocationRefUids.keySet().removeIf(lockEndpointUUIDBiTuple -> lockEndpointUUIDBiTuple.element1.equals(endpoint));
    }

    private Collection<LockInvocationKey> setNewLockOwner() {
        Collection<LockInvocationKey> newOwnerWaitKeys;
        Iterator<WaitKeyContainer<LockInvocationKey>> iter = waitKeyContainersIterator();
        if (iter.hasNext()) {
            WaitKeyContainer<LockInvocationKey> container = iter.next();
            LockInvocationKey newOwner = container.key();
            newOwnerWaitKeys = container.keyAndRetries();

            iter.remove();
            owner = newOwner;
            lockCount = 1;
            ownerInvocationRefUids.put(BiTuple.of(owner.endpoint(), owner.invocationUid()), lockOwnershipState());
        } else {
            owner = null;
            newOwnerWaitKeys = Collections.emptyList();
        }

        return newOwnerWaitKeys;
    }

    RaftLockOwnershipState lockOwnershipState() {
        if (owner == null) {
            return RaftLockOwnershipState.NOT_LOCKED;
        }

        return new RaftLockOwnershipState(owner.commitIndex(), lockCount, owner.sessionId(), owner.endpoint().threadId());
    }

    RaftLock cloneForSnapshot() {
        RaftLock clone = new RaftLock();
        cloneForSnapshot(clone);
        clone.lockCountLimit = this.lockCountLimit;
        clone.owner = this.owner;
        clone.lockCount = this.lockCount;
        clone.ownerInvocationRefUids.putAll(this.ownerInvocationRefUids);

        return clone;
    }

    /**
     * Releases the lock if the current lock holder's session is closed.
     */
    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
        removeInvocationRefUids(sessionId);

        if (owner != null && owner.sessionId() == sessionId) {
            ReleaseResult result = doRelease(owner.endpoint(), newUnsecureUUID(), lockCount);
            for (LockInvocationKey key : result.completedWaitKeys()) {
                responses.put(key.commitIndex(), result.ownership().getFence());
            }
        }
    }

    private void removeInvocationRefUids(long sessionId) {
        ownerInvocationRefUids.keySet()
                              .removeIf(lockEndpointUUIDBiTuple -> lockEndpointUUIDBiTuple.element1.sessionId() == sessionId);
    }

    /**
     * Returns session id of the current lock holder or an empty collection if
     * the lock is not held
     */
    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.emptyList();
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftLockDataSerializerHook.RAFT_LOCK;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(lockCountLimit);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        out.writeInt(ownerInvocationRefUids.size());
        for (Map.Entry<BiTuple<LockEndpoint, UUID>, RaftLockOwnershipState> e : ownerInvocationRefUids.entrySet()) {
            out.writeObject(e.getKey().element1);
            writeUUID(out, e.getKey().element2);
            out.writeObject(e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        lockCountLimit = in.readInt();
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        int ownerInvocationRefUidCount = in.readInt();
        for (int i = 0; i < ownerInvocationRefUidCount; i++) {
            LockEndpoint endpoint = in.readObject();
            UUID invocationUid = readUUID(in);
            RaftLockOwnershipState ownership = in.readObject();
            ownerInvocationRefUids.put(BiTuple.of(endpoint, invocationUid), ownership);
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + internalToString() + ", lockCountLimit=" + lockCountLimit + ", owner="
                + owner + ", lockCount=" + lockCount + ", ownerInvocationRefUids=" + ownerInvocationRefUids + '}';
    }

}
