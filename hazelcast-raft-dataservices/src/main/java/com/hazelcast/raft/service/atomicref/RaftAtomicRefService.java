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

package com.hazelcast.raft.service.atomicref;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.atomicref.proxy.RaftAtomicRefProxy;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.raft.impl.service.RaftService.getObjectNameForProxy;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

/**
 * Contains Raft-based atomic reference instances, implements snapshotting, and creates proxies
 */
public class RaftAtomicRefService implements ManagedService, RaftRemoteService, RaftGroupLifecycleAwareService,
                                             SnapshotAwareService<RaftAtomicRefSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicRefService";

    private final Map<Tuple2<RaftGroupId, String>, RaftAtomicRef> atomicRefs =
            new ConcurrentHashMap<Tuple2<RaftGroupId, String>, RaftAtomicRef>();
    private final Set<Tuple2<RaftGroupId, String>> destroyedRefs =
            newSetFromMap(new ConcurrentHashMap<Tuple2<RaftGroupId, String>, Boolean>());
    private final NodeEngine nodeEngine;
    private volatile RaftService raftService;

    public RaftAtomicRefService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public RaftAtomicRefSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Data> refs = new HashMap<String, Data>();
        for (RaftAtomicRef ref : atomicRefs.values()) {
            if (ref.groupId().equals(groupId)) {
                refs.put(ref.name(), ref.get());
            }
        }

        Set<String> destroyed = new HashSet<String>();
        for (Tuple2<RaftGroupId, String> tuple : destroyedRefs) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return new RaftAtomicRefSnapshot(refs, destroyed);
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, RaftAtomicRefSnapshot snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, Data> e : snapshot.getRefs()) {
            String name = e.getKey();
            Data val = e.getValue();
            atomicRefs.put(Tuple2.of(groupId, name), new RaftAtomicRef(groupId, name, val));
        }

        for (String name : snapshot.getDestroyed()) {
            destroyedRefs.add(Tuple2.of(groupId, name));
        }
    }

    @Override
    public void onGroupDestroy(RaftGroupId groupId) {
        Iterator<Tuple2<RaftGroupId, String>> iter = atomicRefs.keySet().iterator();
        while (iter.hasNext()) {
            Tuple2<RaftGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                destroyedRefs.add(next);
                iter.remove();
            }
        }
    }

    @Override
    public IAtomicReference createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = raftService.createRaftGroupForProxy(name);
            RaftInvocationManager invocationManager = raftService.getInvocationManager();
            SerializationService serializationService = nodeEngine.getSerializationService();
            return new RaftAtomicRefProxy(invocationManager, serializationService, groupId, getObjectNameForProxy(name));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        destroyedRefs.add(key);
        return atomicRefs.remove(key) != null;
    }

    public RaftAtomicRef getAtomicRef(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        if (destroyedRefs.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicReference[" + name + "] is already destroyed!");
        }
        RaftAtomicRef atomicRef = atomicRefs.get(key);
        if (atomicRef == null) {
            atomicRef = new RaftAtomicRef(groupId, groupId.name());
            atomicRefs.put(key, atomicRef);
        }
        return atomicRef;
    }
}
