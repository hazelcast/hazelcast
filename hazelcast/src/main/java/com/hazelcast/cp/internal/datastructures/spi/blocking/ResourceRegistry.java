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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Base class for registries for blocking resources.
 * Maintains blocking resources and their associated wait timeouts.
 * When a new wait key is registered into a blocking resource, a timeout can be
 * set here. If a return value is not produced for that wait key until
 * the timeout occurs, the wait key will be expired.
 *
 * @param <W> concrete type of the WaitKey
 * @param <R> concrete type of the resource
 */
public abstract class ResourceRegistry<W extends WaitKey, R extends BlockingResource<W>>
        implements LiveOperationsTracker, DataSerializable {

    protected CPGroupId groupId;
    protected final Map<String, R> resources = new ConcurrentHashMap<>();
    protected final Set<String> destroyedNames = new HashSet<>();
    // key.element1: name, key.element2: invocation uid
    // value.element1: timeout duration (persisted in the snapshot), value.element2: deadline timestamp (transient)
    protected final ConcurrentMap<BiTuple<String, UUID>, BiTuple<Long, Long>> waitTimeouts = new ConcurrentHashMap<>();

    // Live operations are not put into Raft snapshots because it is not needed.
    // Currently, only Raft ops that create a wait key are tracked as live operations,
    // and it is sufficiently to report them only through the leader. Although followers
    // populate live operations as well, they can miss some entries if they install a snapshot
    // instead of applying Raft log entries. If a follower becomes later, callers will
    // retry and commit their waiting Raft ops.
    private final Set<BiTuple<Address, Long>> liveOperationsSet = newSetFromMap(new ConcurrentHashMap<>());

    protected ResourceRegistry() {
    }

    protected ResourceRegistry(CPGroupId groupId) {
        this.groupId = groupId;
    }

    protected abstract R createNewResource(CPGroupId groupId, String name);

    protected abstract ResourceRegistry<W, R> cloneForSnapshot();

    // public only for testing purposes
    public final R getResourceOrNull(String name) {
        checkNotDestroyed(name);
        return resources.get(name);
    }

    protected final R getOrInitResource(String name) {
        checkNotDestroyed(name);
        R resource = resources.get(name);
        if (resource == null) {
            resource = createNewResource(groupId, name);
            resources.put(name, resource);
        }

        return resource;
    }

    private void checkNotDestroyed(String name) {
        checkNotNull(name);
        if (destroyedNames.contains(name)) {
            throw new DistributedObjectDestroyedException("Resource[" + name + "] is already destroyed!");
        }
    }

    protected final void addWaitKey(String name, W key, long timeoutMs) {
        if (timeoutMs > 0) {
            long deadline = Clock.currentTimeMillis() + timeoutMs;
            waitTimeouts.putIfAbsent(BiTuple.of(name, key.invocationUid), BiTuple.of(timeoutMs, deadline));
        }
        if (timeoutMs != 0) {
            addLiveOperation(key);
        }
    }

    protected final void removeWaitKey(String name, W key) {
        waitTimeouts.remove(BiTuple.of(name, key.invocationUid()));
        removeLiveOperation(key);
    }

    final void expireWaitKey(String name, UUID invocationUid, List<W> expired) {
        waitTimeouts.remove(BiTuple.of(name, invocationUid));

        BlockingResource<W> resource = getResourceOrNull(name);
        if (resource != null) {
            resource.expireWaitKeys(invocationUid, expired);
        }
    }

    final Collection<BiTuple<String, UUID>> getWaitKeysToExpire(long now) {
        List<BiTuple<String, UUID>> expired = new ArrayList<>();
        for (Entry<BiTuple<String, UUID>, BiTuple<Long, Long>> e : waitTimeouts.entrySet()) {
            long deadline = e.getValue().element2;
            if (deadline <= now) {
                expired.add(e.getKey());
            }
        }

        return expired;
    }

    final Map<BiTuple<String, UUID>, Long> overwriteWaitTimeouts(Map<BiTuple<String, UUID>, BiTuple<Long, Long>>
                                                                        existingWaitTimeouts) {
        for (Entry<BiTuple<String, UUID>, BiTuple<Long, Long>> e : existingWaitTimeouts.entrySet()) {
            waitTimeouts.put(e.getKey(), e.getValue());
        }

        Map<BiTuple<String, UUID>, Long> newKeys = new HashMap<>();
        for (Entry<BiTuple<String, UUID>, BiTuple<Long, Long>> e : waitTimeouts.entrySet()) {
            BiTuple<String, UUID> key = e.getKey();
            if (!existingWaitTimeouts.containsKey(key)) {
                Long timeout = e.getValue().element1;
                newKeys.put(key, timeout);
            }
        }

        return newKeys;
    }

    final void closeSession(long sessionId, List<Long> expiredWaitKeys, Map<Long, Object> result) {
        for (R resource : resources.values()) {
            resource.closeSession(sessionId, expiredWaitKeys, result);
        }
    }

    final Collection<Long> getAttachedSessions() {
        Set<Long> sessions = new HashSet<>();
        for (R res : resources.values()) {
            res.collectAttachedSessions(sessions);
        }

        return sessions;
    }

    final Collection<W> destroyResource(String name) {
        destroyedNames.add(name);
        BlockingResource<W> resource = resources.remove(name);
        if (resource == null) {
            return null;
        }

        Collection<W> keys = resource.getAllWaitKeys();
        for (W key : keys) {
            removeWaitKey(name, key);
        }

        return keys;
    }

    public final CPGroupId getGroupId() {
        return groupId;
    }

    // queried locally in tests
    public final Map<BiTuple<String, UUID>, BiTuple<Long, Long>> getWaitTimeouts() {
        return unmodifiableMap(waitTimeouts);
    }

    public final Collection<Long> destroy() {
        destroyedNames.addAll(resources.keySet());
        Collection<Long> indices = new ArrayList<>();
        for (BlockingResource<W> raftLock : resources.values()) {
            for (W key : raftLock.getAllWaitKeys()) {
                indices.add(key.commitIndex());
            }
        }

        resources.clear();
        waitTimeouts.clear();

        return indices;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (BiTuple<Address, Long> t : liveOperationsSet) {
            liveOperations.add(t.element1, t.element2);
        }
    }

    private void addLiveOperation(W key) {
        liveOperationsSet.add(BiTuple.of(key.callerAddress(), key.callId()));
    }

    final void removeLiveOperation(W key) {
        liveOperationsSet.remove(BiTuple.of(key.callerAddress(), key.callId()));
    }

    public final Collection<BiTuple<Address, Long>> getLiveOperations() {
        return liveOperationsSet;
    }

    final void onSnapshotRestore() {
        for (R resource : resources.values()) {
            for (W key : resource.getAllWaitKeys()) {
                addLiveOperation(key);
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeInt(resources.size());
        for (Entry<String, R> e : resources.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeObject(e.getValue());
        }
        out.writeInt(destroyedNames.size());
        for (String name : destroyedNames) {
            out.writeUTF(name);
        }
        out.writeInt(waitTimeouts.size());
        for (Entry<BiTuple<String, UUID>, BiTuple<Long, Long>> e : waitTimeouts.entrySet()) {
            BiTuple<String, UUID> t = e.getKey();
            out.writeUTF(t.element1);
            writeUUID(out, t.element2);
            out.writeLong(e.getValue().element1);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readUTF();
            R res = in.readObject();
            resources.put(name, res);
        }
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readUTF();
            destroyedNames.add(name);
        }
        long now = Clock.currentTimeMillis();
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readUTF();
            UUID invocationUid = readUUID(in);
            long timeout = in.readLong();
            waitTimeouts.put(BiTuple.of(name, invocationUid), BiTuple.of(timeout, now + timeout));
        }
    }

    @Override
    public String toString() {
        return "ResourceRegistry{" + "groupId=" + groupId + ", resources=" + resources + ", destroyedNames=" + destroyedNames
                + ", waitTimeouts=" + waitTimeouts + '}';
    }
}
