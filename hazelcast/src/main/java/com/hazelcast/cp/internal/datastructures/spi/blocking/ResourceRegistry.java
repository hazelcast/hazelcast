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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.operationservice.LiveOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
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
        implements DataSerializable {

    // If the wait key of a blocking call times out, it is still reported
    // as a live operation. This could lead to hang the caller side when
    // the majority is lost because the Raft invocation does not fail with
    // operation timeout. To prevent this problem, we don't report a wait
    // key as a live operation if it is not expired on the CP group after
    // its wait timeout occurs. However, if we stop reporting a wait key
    // immediately after its timeout, then the caller might receive
    // an operation timeout prematurely, even though the majority is alive
    // and the wait key is expired on the CP group gracefully. In order to
    // prevent this problem, we extend the wait key timeout a few more seconds
    // so that the expiration logic could kick in and expire the key gracefully
    // when the CP group majority is healthy.
    private static final long OPERATION_TIMEOUT_EXTENSION_MS = TimeUnit.SECONDS.toMillis(5);

    // If no timeout is given for a blocking call, then we will use
    // this constant to not to expire its wait key
    private static final long NO_WAIT_KEY_DEADLINE = Long.MAX_VALUE;


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
    // instead of applying Raft log entries. If a follower becomes leader later on, callers will
    // retry and commit their waiting Raft ops.
    // The values are the deadlines of the wait keys plus the OPERATION_TIMEOUT_EXTENSION_MS value.
    // If a wait key times out but still not expired on the CP group for any reason, such as lost majority,
    // it is no longer reported as a live operation so that the caller could give up on waiting.
    private final Map<BiTuple<Address, Long>, Long> liveOperationMap = new ConcurrentHashMap<>();

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
        long deadline;
        if (timeoutMs > 0) {
            long now = Clock.currentTimeMillis();
            deadline = Long.MAX_VALUE - now >= timeoutMs ? now + timeoutMs : Long.MAX_VALUE;
            waitTimeouts.putIfAbsent(BiTuple.of(name, key.invocationUid), BiTuple.of(timeoutMs, deadline));
        } else {
            deadline = NO_WAIT_KEY_DEADLINE;
        }

        if (timeoutMs != 0) {
            addLiveOperation(key, deadline);
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

    public void populate(LiveOperations liveOperations, long now) {
        Iterator<Entry<BiTuple<Address, Long>, Long>> it = liveOperationMap.entrySet().iterator();
        while (it.hasNext()) {
            Entry<BiTuple<Address, Long>, Long> e = it.next();
            long deadline = e.getValue();
            if (deadline >= now) {
                BiTuple<Address, Long> t = e.getKey();
                liveOperations.add(t.element1, t.element2);
            } else {
                it.remove();
            }
        }
    }

    private void addLiveOperation(W key, long deadline) {
        // if deadline is NO_WAIT_KEY_DEADLINE, it is already Long.MAX_VALUE
        // and no need to extend it further.
        if (Long.MAX_VALUE - deadline >= OPERATION_TIMEOUT_EXTENSION_MS) {
            deadline += OPERATION_TIMEOUT_EXTENSION_MS;
        }

        liveOperationMap.put(BiTuple.of(key.callerAddress(), key.callId()), deadline);
    }

    final void removeLiveOperation(W key) {
        liveOperationMap.remove(BiTuple.of(key.callerAddress(), key.callId()));
    }

    public final Collection<BiTuple<Address, Long>> getLiveOperations() {
        return liveOperationMap.keySet();
    }

    final void onSnapshotRestore() {
        for (R resource : resources.values()) {
            for (W key : resource.getAllWaitKeys()) {
                BiTuple<Long, Long> t = waitTimeouts.get(BiTuple.of(resource.getName(), key.invocationUid));
                long deadline = t != null ? t.element1 : NO_WAIT_KEY_DEADLINE;
                addLiveOperation(key, deadline);
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeInt(resources.size());
        for (Entry<String, R> e : resources.entrySet()) {
            out.writeString(e.getKey());
            out.writeObject(e.getValue());
        }
        out.writeInt(destroyedNames.size());
        for (String name : destroyedNames) {
            out.writeString(name);
        }
        out.writeInt(waitTimeouts.size());
        for (Entry<BiTuple<String, UUID>, BiTuple<Long, Long>> e : waitTimeouts.entrySet()) {
            BiTuple<String, UUID> t = e.getKey();
            out.writeString(t.element1);
            writeUUID(out, t.element2);
            out.writeLong(e.getValue().element1);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readString();
            R res = in.readObject();
            resources.put(name, res);
        }
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readString();
            destroyedNames.add(name);
        }
        long now = Clock.currentTimeMillis();
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            String name = in.readString();
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
