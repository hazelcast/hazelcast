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
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
class LockRegistry {

    private final RaftGroupId groupId;
    private final Map<String, RaftLock> locks = new HashMap<String, RaftLock>();
    private final Set<String> destroyedLockNames = new HashSet<String>();
    // value.element1: timeout duration, value.element2: deadline (transient)
    private final Map<LockInvocationKey, Tuple2<Long, Long>> tryLockTimeouts
            = new ConcurrentHashMap<LockInvocationKey, Tuple2<Long, Long>>();

    LockRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    public RaftGroupId groupId() {
        return groupId;
    }

    // element1: invalidated wait entries
    // element2: lock-acquired wait entries
    Tuple2<Collection<Long>, Collection<Long>> invalidateSession(long sessionId) {
        List<Long> invalidations = new ArrayList<Long>();
        List<Long> acquires = new ArrayList<Long>();
        for (Entry<String, RaftLock> entry : locks.entrySet()) {
            RaftLock lock = entry.getValue();

            List<Long> indices = lock.invalidateWaitEntries(sessionId);
            invalidations.addAll(indices);

            LockInvocationKey owner = lock.owner();
            if (owner != null && sessionId == owner.endpoint().sessionId()) {
                Collection<LockInvocationKey> w = lock.release(owner.endpoint(), Integer.MAX_VALUE, UuidUtil.newUnsecureUUID());
                for (LockInvocationKey waitEntry : w) {
                    acquires.add(waitEntry.commitIndex());
                }
            }
        }

        return Tuple2.<Collection<Long>, Collection<Long>>of(invalidations, acquires);
    }

    private RaftLock getOrInitRaftLock(String name) {
        checkLockNotDestroyed(name);
        RaftLock raftLock = locks.get(name);
        if (raftLock == null) {
            raftLock = new RaftLock(groupId, name);
            locks.put(name, raftLock);
        }
        return raftLock;
    }

    private RaftLock getRaftLockOrNull(String name) {
        checkLockNotDestroyed(name);
        return locks.get(name);
    }

    private void checkLockNotDestroyed(String name) {
        checkNotNull(name);
        if (destroyedLockNames.contains(name)) {
            throw new DistributedObjectDestroyedException("Lock[" + name + "] is already destroyed!");
        }
    }

    boolean acquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        return getOrInitRaftLock(name).acquire(endpoint, commitIndex, invocationUid, true);
    }

    boolean tryAcquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid, long timeoutMs) {
        boolean wait = (timeoutMs > 0);
        boolean acquired = getOrInitRaftLock(name).acquire(endpoint, commitIndex, invocationUid, wait);
        if (wait && !acquired) {
            LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
            tryLockTimeouts.put(key, Tuple2.of(timeoutMs, Clock.currentTimeMillis() + timeoutMs));
        }

        return acquired;
    }

    Collection<LockInvocationKey> release(String name, LockEndpoint endpoint, UUID invocationUid) {
        RaftLock lock = getRaftLockOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> waitKeys = lock.release(endpoint, invocationUid);
        for (LockInvocationKey waitKey : waitKeys) {
            tryLockTimeouts.remove(waitKey);
        }

        return waitKeys;
    }

    Collection<LockInvocationKey> forceRelease(String name, long expectedFence, UUID invocationUid) {
        RaftLock lock = getRaftLockOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> waitKeys = lock.forceRelease(expectedFence, invocationUid);
        for (LockInvocationKey waitKey : waitKeys) {
            tryLockTimeouts.remove(waitKey);
        }

        return waitKeys;
    }

    boolean invalidateWaitEntry(LockInvocationKey key) {
        RaftLock lock = getRaftLockOrNull(key.name());
        if (lock == null) {
            return false;
        }

        tryLockTimeouts.remove(key);
        return lock.invalidateWaitEntry(key);
    }

    int getLockCount(String name, LockEndpoint endpoint) {
        RaftLock lock = getRaftLockOrNull(name);
        if (lock == null) {
            return 0;
        }

        if (endpoint != null) {
            LockInvocationKey owner = lock.owner();
            return (owner != null && endpoint.equals(owner.endpoint())) ? lock.lockCount() : 0;
        }

        return lock.lockCount();
    }

    long getLockFence(String name) {
        RaftLock lock = getRaftLockOrNull(name);
        if (lock == null) {
            throw new IllegalMonitorStateException();
        }

        LockInvocationKey owner = lock.owner();
        if (owner == null) {
            throw new IllegalMonitorStateException("Lock[" + name + "] has no owner!");
        }

        return owner.commitIndex();
    }

    Collection<LockInvocationKey> getExpiredWaitEntries(long now) {
        List<LockInvocationKey> expired = new ArrayList<LockInvocationKey>();
        for (Entry<LockInvocationKey, Tuple2<Long, Long>> e : tryLockTimeouts.entrySet()) {
            long deadline = e.getValue().element2;
            if (deadline <= now) {
                expired.add(e.getKey());
            }
        }

        return expired;
    }

    // queried locally in tests
    Map<LockInvocationKey, Tuple2<Long, Long>> getTryLockTimeouts() {
        return tryLockTimeouts;
    }

    LockRegistrySnapshot toSnapshot() {
        return new LockRegistrySnapshot(locks.values(), tryLockTimeouts, destroyedLockNames);
    }

    Map<LockInvocationKey, Long> restore(LockRegistrySnapshot snapshot) {
        for (RaftLockSnapshot lockSnapshot : snapshot.getLocks()) {
            locks.put(lockSnapshot.getName(), new RaftLock(lockSnapshot));
        }

        destroyedLockNames.addAll(snapshot.getDestroyedLockNames());

        long now = Clock.currentTimeMillis();
        Map<LockInvocationKey, Long> added = new HashMap<LockInvocationKey, Long>();
        for (Entry<LockInvocationKey, Long> e : snapshot.getTryLockTimeouts().entrySet()) {
            LockInvocationKey key = e.getKey();
            if (!tryLockTimeouts.containsKey(key)) {
                long timeout = e.getValue();
                tryLockTimeouts.put(key, Tuple2.of(timeout, now + timeout));
                added.put(key, timeout);
            }
        }

        return added;
    }

    Collection<Long> destroyLock(String name) {
        destroyedLockNames.add(name);
        RaftLock raftLock = locks.remove(name);
        if (raftLock == null) {
            return null;
        }

        Collection<Long> indices = new ArrayList<Long>(raftLock.waitEntries().size());
        for (LockInvocationKey key : raftLock.waitEntries()) {
            indices.add(key.commitIndex());
            tryLockTimeouts.remove(key);
        }
        return indices;
    }

    Collection<Long> destroy() {
        destroyedLockNames.addAll(locks.keySet());
        Collection<Long> indices = new ArrayList<Long>();
        for (RaftLock raftLock : locks.values()) {
            for (LockInvocationKey key : raftLock.waitEntries()) {
                indices.add(key.commitIndex());
            }
        }
        locks.clear();
        tryLockTimeouts.clear();
        return indices;
    }
}
