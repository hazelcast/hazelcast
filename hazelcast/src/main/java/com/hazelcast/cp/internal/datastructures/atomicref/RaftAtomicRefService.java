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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.RaftAtomicRefProxy;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

/**
 * Contains Raft-based atomic reference instances, implements snapshotting,
 * and creates proxies
 */
public class RaftAtomicRefService implements RaftManagedService, RaftRemoteService, RaftNodeLifecycleAwareService,
                                             SnapshotAwareService<RaftAtomicRefSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicRefService";

    private final Map<Tuple2<CPGroupId, String>, RaftAtomicRef> atomicRefs = new ConcurrentHashMap<>();
    private final Set<Tuple2<CPGroupId, String>> destroyedRefs = newSetFromMap(new ConcurrentHashMap<>());
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
    public void onCPSubsystemRestart() {
        atomicRefs.clear();
        destroyedRefs.clear();
    }

    @Override
    public RaftAtomicRefSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Data> refs = new HashMap<>();
        for (RaftAtomicRef ref : atomicRefs.values()) {
            if (ref.groupId().equals(groupId)) {
                refs.put(ref.name(), ref.get());
            }
        }

        Set<String> destroyed = new HashSet<>();
        for (Tuple2<CPGroupId, String> tuple : destroyedRefs) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return new RaftAtomicRefSnapshot(refs, destroyed);
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, RaftAtomicRefSnapshot snapshot) {
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
    public void onRaftGroupDestroyed(CPGroupId groupId) {
        Iterator<Tuple2<CPGroupId, String>> iter = atomicRefs.keySet().iterator();
        while (iter.hasNext()) {
            Tuple2<CPGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                destroyedRefs.add(next);
                iter.remove();
            }
        }
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public boolean destroyRaftObject(CPGroupId groupId, String name) {
        Tuple2<CPGroupId, String> key = Tuple2.of(groupId, name);
        destroyedRefs.add(key);
        return atomicRefs.remove(key) != null;
    }

    public RaftAtomicRef getAtomicRef(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<CPGroupId, String> key = Tuple2.of(groupId, name);
        if (destroyedRefs.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicReference[" + name + "] is already destroyed!");
        }
        RaftAtomicRef atomicRef = atomicRefs.get(key);
        if (atomicRef == null) {
            atomicRef = new RaftAtomicRef(groupId, name);
            atomicRefs.put(key, atomicRef);
        }
        return atomicRef;
    }

    @Override
    public IAtomicReference createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return new RaftAtomicRefProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
