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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.util.Tuple2;
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
 * Contains Raft-based atomic long instances, implements snapshotting,
 * and creates proxies
 */
public class RaftAtomicLongService implements RaftManagedService, RaftRemoteService, RaftNodeLifecycleAwareService,
                                              SnapshotAwareService<RaftAtomicLongSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicLongService";

    private final Map<Tuple2<CPGroupId, String>, RaftAtomicLong> atomicLongs = new ConcurrentHashMap<>();
    private final Set<Tuple2<CPGroupId, String>> destroyedLongs = newSetFromMap(new ConcurrentHashMap<>());
    private final NodeEngine nodeEngine;
    private volatile RaftService raftService;

    public RaftAtomicLongService(NodeEngine nodeEngine) {
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
        atomicLongs.clear();
    }

    @Override
    public void onCPSubsystemRestart() {
        atomicLongs.clear();
        destroyedLongs.clear();
    }

    @Override
    public RaftAtomicLongSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Long> longs = new HashMap<>();
        for (RaftAtomicLong atomicLong : atomicLongs.values()) {
            if (atomicLong.groupId().equals(groupId)) {
                longs.put(atomicLong.name(), atomicLong.value());
            }
        }

        Set<String> destroyed = new HashSet<>();
        for (Tuple2<CPGroupId, String> tuple : destroyedLongs) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return new RaftAtomicLongSnapshot(longs, destroyed);
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, RaftAtomicLongSnapshot snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, Long> e : snapshot.getLongs()) {
            String name = e.getKey();
            long val = e.getValue();
            atomicLongs.put(Tuple2.of(groupId, name), new RaftAtomicLong(groupId, name, val));
        }

        for (String name : snapshot.getDestroyed()) {
            destroyedLongs.add(Tuple2.of(groupId, name));
        }
    }

    @Override
    public boolean destroyRaftObject(CPGroupId groupId, String name) {
        Tuple2<CPGroupId, String> key = Tuple2.of(groupId, name);
        destroyedLongs.add(key);
        return atomicLongs.remove(key) != null;
    }

    @Override
    public void onRaftGroupDestroyed(CPGroupId groupId) {
        Iterator<Tuple2<CPGroupId, String>> iter = atomicLongs.keySet().iterator();
        while (iter.hasNext()) {
            Tuple2<CPGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                destroyedLongs.add(next);
                iter.remove();
            }
        }
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    public RaftAtomicLong getAtomicLong(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<CPGroupId, String> key = Tuple2.of(groupId, name);
        if (destroyedLongs.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicLong[" + name + "] is already destroyed!");
        }
        RaftAtomicLong atomicLong = atomicLongs.get(key);
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(groupId, name);
            atomicLongs.put(key, atomicLong);
        }
        return atomicLong;
    }

    @Override
    public IAtomicLong createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return new RaftAtomicLongProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

}
