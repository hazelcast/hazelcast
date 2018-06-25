/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockOwnershipState.NOT_LOCKED;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.unmodifiableCollection;

/**
 * State-machine implementation of the Raft-based lock
 */
class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    /**
     * Current owner of the lock
     */
    private LockInvocationKey owner;

    /**
     * Number of acquires the current lock owner has committed on the Raft group
     */
    private int lockCount;

    /**
     * For each LockEndpoint, uid of its last invocation.
     * Used for preventing duplicate execution of lock / unlock requests.
     */
    private Map<LockEndpoint, UUID> invocationRefUids = new HashMap<LockEndpoint, UUID>();

    RaftLock() {
    }

    RaftLock(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    /**
     * Assigns the lock to the endpoint, if the lock is not held.
     * Lock count is incremented if the endpoint already holds the lock.
     * If some other endpoint holds the lock and the second argument is true, a wait key is created and added to the wait queue.
     * Lock count is not incremented if the lock request is a retry of the lock holder.
     * If the lock request is a retry of a lock endpoint that resides in the wait queue with the same invocation uid,
     * a duplicate wait key is added to the wait queue because cancelling the previous wait key can cause the caller to fail.
     * If the lock request is a new request of a lock endpoint that resides in the wait queue with a different invocation uid,
     * the existing wait key is cancelled because it means the caller has stopped waiting for response of the previous invocation.
     */
    AcquireResult acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if lock() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))
                || (owner != null && owner.invocationUid().equals(invocationUid))) {
            RaftLockOwnershipState ownership = new RaftLockOwnershipState(owner.commitIndex(), lockCount, endpoint.sessionId(),
                    endpoint.threadId());
            return AcquireResult.acquired(ownership);
        }

        invocationRefUids.remove(endpoint);

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);
            lockCount++;
            RaftLockOwnershipState ownership = new RaftLockOwnershipState(owner.commitIndex(), lockCount, endpoint.sessionId(),
                    endpoint.threadId());
            return AcquireResult.acquired(ownership);
        }

        Collection<LockInvocationKey> cancelledWaitKeys = cancelWaitKeys(endpoint, invocationUid);

        if (wait) {
            waitKeys.add(key);
        }

        return AcquireResult.notAcquired(cancelledWaitKeys);
    }

    private Collection<LockInvocationKey> cancelWaitKeys(LockEndpoint endpoint, UUID invocationUid) {
        List<LockInvocationKey> cancelled = new ArrayList<LockInvocationKey>(0);
        Iterator<LockInvocationKey> it = waitKeys.iterator();
        while (it.hasNext()) {
            LockInvocationKey waitKey = it.next();
            if (waitKey.endpoint().equals(endpoint) && !waitKey.invocationUid().equals(invocationUid)) {
                cancelled.add(waitKey);
                it.remove();
            }
        }

        return cancelled;
    }

    /**
     * Releases the lock with the given release count. The lock is freed if release count > lock count.
     * If the remaining lock count > 0 after a successful release, the lock is still held by the endpoint.
     * The lock is not released if it is a retry of a previous successful release request of the current lock holder.
     * If the lock is assigned to some other endpoint after this release, wait keys of the new lock holder are returned.
     * If the release request fails because the requesting endpoint does not hold the lock, all wait keys of the endpoint
     * are cancelled because that endpoint has stopped waiting for response of the previous lock() invocation.
     */
    ReleaseResult release(LockEndpoint endpoint, UUID invocationUid, int releaseCount) {
        // if unlock() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))) {
            return ReleaseResult.successful(lockOwnershipState());
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return ReleaseResult.successful(lockOwnershipState());
            }

            List<LockInvocationKey> keys;
            LockInvocationKey newOwner = waitKeys.poll();
            if (newOwner != null) {
                keys = new ArrayList<LockInvocationKey>(1);
                keys.add(newOwner);

                Iterator<LockInvocationKey> iter = waitKeys.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey key = iter.next();
                    if (newOwner.invocationUid().equals(key.invocationUid())) {
                        assert newOwner.endpoint().equals(key.endpoint());
                        keys.add(key);
                        iter.remove();
                    }
                }

                owner = newOwner;
                lockCount = 1;
            } else {
                owner = null;
                keys = Collections.emptyList();
            }

            return ReleaseResult.successful(lockOwnershipState(), keys);
        }

        return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
    }

    /**
     * Releases the lock if it is current held by the given expected fencing token.
     */
    ReleaseResult forceRelease(long expectedFence, UUID invocationUid) {
        if (owner != null && owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), invocationUid, lockCount);
        }

        return ReleaseResult.FAILED;
    }

    RaftLockOwnershipState lockOwnershipState() {
        if (owner == null) {
            return RaftLockOwnershipState.NOT_LOCKED;
        }

        return new RaftLockOwnershipState(owner.commitIndex(), lockCount, owner.sessionId(), owner.endpoint().threadId());
    }

    /**
     * Releases the lock if the current lock holder's session is closed.
     */
    @Override
    protected void onSessionClose(long sessionId, Long2ObjectHashMap<Object> responses) {
        if (owner != null && sessionId == owner.endpoint().sessionId()) {
            Iterator<LockEndpoint> it = invocationRefUids.keySet().iterator();
            while (it.hasNext()) {
                if (it.next().sessionId() == sessionId) {
                    it.remove();
                }
            }

            ReleaseResult result = release(owner.endpoint(), newUnsecureUUID(), Integer.MAX_VALUE);

            if (!result.success) {
                assert result.notifications.isEmpty();
                return;
            }

            Collection<LockInvocationKey> notifications = result.notifications;
            if (notifications.size() > 0) {
                Object newOwnerCommitIndex = notifications.iterator().next().commitIndex();
                for (LockInvocationKey waitKey : notifications) {
                    responses.put(waitKey.commitIndex(), newOwnerCommitIndex);
                }
            }
        }
    }

    /**
     * Returns session id of the current lock holder or an empty collection if the lock is not held
     */
    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.<Long>emptyList();
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        out.writeInt(invocationRefUids.size());
        for (Map.Entry<LockEndpoint, UUID> e : invocationRefUids.entrySet()) {
            out.writeObject(e.getKey());
            UUID releaseUid = e.getValue();
            out.writeLong(releaseUid.getLeastSignificantBits());
            out.writeLong(releaseUid.getMostSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        int releaseRefUidCount = in.readInt();
        for (int i = 0; i < releaseRefUidCount; i++) {
            LockEndpoint endpoint = in.readObject();
            long least = in.readLong();
            long most = in.readLong();
            invocationRefUids.put(endpoint, new UUID(most, least));
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + "groupId=" + groupId + ", name='" + name + '\'' + ", owner=" + owner + ", lockCount=" + lockCount
                + ", invocationRefUids=" + invocationRefUids + ", waitKeys=" + waitKeys + '}';
    }

    /**
     * Represents result of a lock() request
     */
    static final class AcquireResult {

        /**
         * If the lock() request is successful, represents new state of the lock ownership.
         * It is {@link RaftLockOwnershipState#NOT_LOCKED} otherwise.
         */
        final RaftLockOwnershipState ownership;

        /**
         * If new a lock() request is send while there are pending wait keys of a previous lock() request,
         * pending wait keys are cancelled. It is because LockEndpoint is a single-threaded entity and
         * a new lock() request implies that the LockEndpoint is no longer interested in its previous lock() call.
         */
        final Collection<LockInvocationKey> cancelled;

        private AcquireResult(RaftLockOwnershipState ownership, Collection<LockInvocationKey> cancelled) {
            this.ownership = ownership;
            this.cancelled = unmodifiableCollection(cancelled);
        }

        private static AcquireResult acquired(RaftLockOwnershipState ownership) {
            return new AcquireResult(ownership, Collections.<LockInvocationKey>emptyList());
        }

        private static AcquireResult notAcquired(Collection<LockInvocationKey> cancelled) {
            return new AcquireResult(NOT_LOCKED, cancelled);
        }
    }

    /**
     * Represents result of a unlock() request
     */
    static final class ReleaseResult {

        static final ReleaseResult FAILED
                = new ReleaseResult(false, NOT_LOCKED, Collections.<LockInvocationKey>emptyList());

        /**
         * true if the unlock() request is successful
         */
        final boolean success;

        /**
         * If the unlock() request is successful, represents new state of the lock ownership.
         * It can be {@link RaftLockOwnershipState#NOT_LOCKED} if the lock has no new owner after successful release.
         * It is {@link RaftLockOwnershipState#NOT_LOCKED} if the unlock() request is failed.
         */
        final RaftLockOwnershipState ownership;

        /**
         * If the unlock() request is successful and ownership is given to some other endpoint, contains its wait keys.
         * If the unlock() request is failed, can contain cancelled wait keys of the caller, if there is any.
         */
        final Collection<LockInvocationKey> notifications;

        private ReleaseResult(boolean success, RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            this.success = success;
            this.ownership = ownership;
            this.notifications = unmodifiableCollection(notifications);
        }

        private static ReleaseResult successful(RaftLockOwnershipState ownership) {
            return new ReleaseResult(true, ownership, Collections.<LockInvocationKey>emptyList());
        }

        private static ReleaseResult successful(RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(true, ownership, notifications);
        }

        private static ReleaseResult failed(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(false, NOT_LOCKED, notifications);
        }
    }

}
