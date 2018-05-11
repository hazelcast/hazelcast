/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.raft.RaftGroupId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock {

    private final RaftGroupId groupId;
    private final String name;

    private LockInvocationKey owner;
    private int lockCount;
    private UUID releaseRefUid;
    private LinkedList<LockInvocationKey> waitEntries = new LinkedList<LockInvocationKey>();

    RaftLock(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftLock(RaftLockSnapshot snapshot) {
        this.groupId = snapshot.getGroupId();
        this.name = snapshot.getName();
        this.owner = snapshot.getOwner();
        this.lockCount = snapshot.getLockCount();
        this.releaseRefUid = snapshot.getRefUid();
        this.waitEntries.addAll(snapshot.getWaitEntries());
    }

    boolean acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (owner != null && owner.invocationUid().equals(invocationUid)) {
            return true;
        }

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            lockCount++;
            return true;
        }

        if (wait) {
            waitEntries.offer(key);
        }

        return false;
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            releaseRefUid = invocationUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return Collections.emptyList();
            }

            LockInvocationKey nextOwner = waitEntries.poll();
            if (nextOwner != null) {
                List<LockInvocationKey> entries = new ArrayList<LockInvocationKey>();
                entries.add(nextOwner);

                Iterator<LockInvocationKey> iter = waitEntries.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey n = iter.next();
                    if (nextOwner.invocationUid().equals(n.invocationUid())) {
                        iter.remove();
                        assert nextOwner.endpoint().equals(n.endpoint());
                        entries.add(n);
                    }
                }

                owner = nextOwner;
                lockCount = 1;
                return entries;
            } else {
                owner = null;
            }

            return Collections.emptyList();
        }

        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    Collection<LockInvocationKey> forceRelease(long expectedFence, UUID invocationUid) {
        // if forceRelease() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner == null) {
            throw new IllegalMonitorStateException();
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), lockCount, invocationUid);
        }

        throw new IllegalMonitorStateException();
    }

    List<Long> invalidateWaitEntries(long sessionId) {
        List<Long> commitIndices = new ArrayList<Long>();
        Iterator<LockInvocationKey> iter = waitEntries.iterator();
        while (iter.hasNext()) {
            LockInvocationKey entry = iter.next();
            if (sessionId == entry.endpoint().sessionId()) {
                commitIndices.add(entry.commitIndex());
                iter.remove();
            }
        }

        return commitIndices;
    }

    boolean invalidateWaitEntry(LockInvocationKey key) {
        Iterator<LockInvocationKey> iter = waitEntries.iterator();
        while (iter.hasNext()) {
            LockInvocationKey waiter = iter.next();
            if (waiter.equals(key)) {
                iter.remove();
                return true;
            }
        }

        return false;
    }

    int lockCount() {
        return lockCount;
    }

    LockInvocationKey owner() {
        return owner;
    }

    Collection<LockInvocationKey> waitEntries() {
        return Collections.unmodifiableCollection(waitEntries);
    }

    RaftLockSnapshot toSnapshot() {
        return new RaftLockSnapshot(groupId, name, owner, lockCount, releaseRefUid, waitEntries);
    }

}
