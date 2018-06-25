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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

/**
 * Base class for registries for blocking resources. Maintains blocking resources and their associated wait timeouts.
 * When a new wait key is registered into a blocking resource, a timeout can be set here.
 * If a return value is not produced for that wait key until the timeout occurs, the wait key will be expired.
 *
 * @param <W> concrete type of the WaitKey
 * @param <R> concrete type of the resource
 */
public abstract class ResourceRegistry<W extends WaitKey, R extends BlockingResource<W>> implements DataSerializable {

    private RaftGroupId groupId;
    private final Map<String, R> resources = new ConcurrentHashMap<String, R>();
    private final Set<String> destroyedNames = new HashSet<String>();
    // value.element1: timeout duration (persisted in the snapshot), value.element2: deadline timestamp (transient)
    private final Map<W, Tuple2<Long, Long>> waitTimeouts = new ConcurrentHashMap<W, Tuple2<Long, Long>>();

    public ResourceRegistry() {
    }

    protected ResourceRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    protected abstract R createNewResource(RaftGroupId groupId, String name);

    protected final R getResourceOrNull(String name) {
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

    protected final void addWaitKey(W key, long timeoutMs) {
        waitTimeouts.put(key, Tuple2.of(timeoutMs, Clock.currentTimeMillis() + timeoutMs));
    }

    protected final void removeWaitKey(W key) {
        waitTimeouts.remove(key);
    }

    final boolean expireWaitKey(W key) {
        removeWaitKey(key);

        BlockingResource<W> resource = getResourceOrNull(key.name());
        if (resource == null) {
            return false;
        }

        return resource.expireWaitKey(key);
    }

    final Collection<W> getWaitKeysToExpire(long now) {
        List<W> expired = new ArrayList<W>();
        for (Entry<W, Tuple2<Long, Long>> e : waitTimeouts.entrySet()) {
            long deadline = e.getValue().element2;
            if (deadline <= now) {
                expired.add(e.getKey());
            }
        }

        return expired;
    }

    final Map<W, Long> overwriteWaitTimeouts(Map<W, Tuple2<Long, Long>> existingWaitTimeouts) {
        for (Entry<W, Tuple2<Long, Long>> e : existingWaitTimeouts.entrySet()) {
            waitTimeouts.put(e.getKey(), e.getValue());
        }

        Map<W, Long> newKeys = new HashMap<W, Long>();
        for (Entry<W, Tuple2<Long, Long>> e : waitTimeouts.entrySet()) {
            W key = e.getKey();
            if (!existingWaitTimeouts.containsKey(key)) {
                Long timeout = e.getValue().element1;
                newKeys.put(key, timeout);
            }
        }

        return newKeys;
    }

    final Map<Long, Object> closeSession(long sessionId) {
        Long2ObjectHashMap<Object> result = new Long2ObjectHashMap<Object>();
        for (R resource : resources.values()) {
            result.putAll(resource.closeSession(sessionId));
        }

        return result;
    }

    final Collection<Long> getAttachedSessions() {
        Set<Long> sessions = new HashSet<Long>();
        for (R res : resources.values()) {
            res.collectAttachedSessions(sessions);
        }

        return sessions;
    }

    final Collection<Long> destroyResource(String name) {
        destroyedNames.add(name);
        BlockingResource<W> resource = resources.remove(name);
        if (resource == null) {
            return null;
        }

        Collection<W> waitKeys = resource.getWaitKeys();
        Collection<Long> indices = new ArrayList<Long>(waitKeys.size());
        for (W key : waitKeys) {
            indices.add(key.commitIndex());
            waitTimeouts.remove(key);
        }

        return indices;
    }

    public final RaftGroupId getGroupId() {
        return groupId;
    }

    // queried locally in tests
    public final Map<W, Tuple2<Long, Long>> getWaitTimeouts() {
        return unmodifiableMap(waitTimeouts);
    }

    public final Collection<Long> destroy() {
        destroyedNames.addAll(resources.keySet());
        Collection<Long> indices = new ArrayList<Long>();
        for (BlockingResource<W> raftLock : resources.values()) {
            for (W key : raftLock.getWaitKeys()) {
                indices.add(key.commitIndex());
            }
        }

        resources.clear();
        waitTimeouts.clear();

        return indices;
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
        for (Entry<W, Tuple2<Long, Long>> e : waitTimeouts.entrySet()) {
            out.writeObject(e.getKey());
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
            W key = in.readObject();
            long timeout = in.readLong();
            waitTimeouts.put(key, Tuple2.of(timeout, now + timeout));
        }
    }

    @Override
    public String toString() {
        return "ResourceRegistry{" + "groupId=" + groupId + ", resources=" + resources + ", destroyedNames=" + destroyedNames
                + ", waitTimeouts=" + waitTimeouts + '}';
    }
}
