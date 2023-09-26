/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.datastructures.spi.AbstractCPMigrationAwareService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hazelcast.cp.internal.RaftService.getCPGroupPartitionId;
import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

/**
 * Contains Raft-based atomic long instances, implements snapshotting,
 * and creates proxies
 */
public class CPMapService
        extends AbstractCPMigrationAwareService
        implements RaftManagedService, RaftRemoteService, RaftNodeLifecycleAwareService,
        SnapshotAwareService<CPMapSnapshot> {
    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:cpMapService";

    protected final Map<BiTuple<CPGroupId, String>, CPMapInternal> maps = new ConcurrentHashMap<>();
    private final Set<BiTuple<CPGroupId, String>> destroyedValues = newSetFromMap(new ConcurrentHashMap<>());
    private volatile RaftService raftService;

    public CPMapService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void reset() {
        if (!raftService.isCpSubsystemEnabled()) {
            clearValues();
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    private void clearValues() {
        maps.clear();
        destroyedValues.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        clearValues();
    }

    @Override
    public void onCPSubsystemRestart() {
        clearValues();
    }

    @Override
    public final CPMapSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Map<Object,Object>> values = new HashMap<>();
        for (CPMapInternal value : maps.values()) {
            if (value.groupId().equals(groupId)) {
                values.put(value.name(), value.map());
            }
        }

        Set<String> destroyed = new HashSet<>();
        for (BiTuple<CPGroupId, String> tuple : destroyedValues) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return newSnapshot(values, destroyed);
    }

    protected  CPMapSnapshot newSnapshot(Map<String, Map<Object,Object>> values, Set<String> destroyed){
        return new CPMapSnapshot(values, destroyed);
    }

    @Override
    public final void restoreSnapshot(CPGroupId groupId, long commitIndex, CPMapSnapshot snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, Map<Object, Object>> e : snapshot.getValues()) {
            String name = e.getKey();
            Map val = e.getValue();
            maps.put(BiTuple.of(groupId, name), newAtomicValue(groupId, name, val));
        }

        for (String name : snapshot.getDestroyed()) {
            destroyedValues.add(BiTuple.of(groupId, name));
        }
    }

    protected CPMapInternal newAtomicValue(CPGroupId groupId, String name, Map<Object,Object> val) {
        return new CPMapInternal(groupId, name, val);
    }

    @Override
    public final void onRaftNodeTerminated(CPGroupId groupId) {
        Iterator<BiTuple<CPGroupId, String>> iter = maps.keySet().iterator();
        while (iter.hasNext()) {
            BiTuple<CPGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                destroyedValues.add(next);
                iter.remove();
            }
        }
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public final boolean destroyRaftObject(CPGroupId groupId, String name) {
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, name);
        destroyedValues.add(key);
        return maps.remove(key) != null;
    }

    public int getMapsCount() {
        return maps.size();
    }

    public final CPMapInternal getCPMapInternal(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, name);
        if (destroyedValues.contains(key)) {
            throw new DistributedObjectDestroyedException("CPMap[" + name + "] is already destroyed!");
        }
        CPMapInternal map = maps.get(key);
        if (map == null) {
            map = newAtomicValue(groupId, name, new HashMap<>());
            maps.put(key, map);
        }
        return map;
    }

    @Override
    public final DistributedObject createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return newRaftAtomicProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected DistributedObject newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId,
                                                   String proxyName, String objectNameForProxy) {
        return new CPMapProxy(nodeEngine, groupId, proxyName, objectNameForProxy);
    }

    @Override
    protected int getBackupCount() {
        return 1;
    }

    @Override
    protected final Map<CPGroupId, Object> getSnapshotMap(int partitionId) {
        assert !raftService.isCpSubsystemEnabled();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return maps.keySet().stream()
                .filter(tuple -> getCPGroupPartitionId(tuple.element1, partitionCount) == partitionId)
                .map(tuple -> tuple.element1)
                .distinct()
                .map(groupId -> BiTuple.of(groupId, takeSnapshot(groupId, 0L)))
                .collect(Collectors.toMap(tuple -> tuple.element1, tuple -> tuple.element2));
    }

    @Override
    protected final void clearPartitionReplica(int partitionId) {
        maps.keySet().removeIf(t -> raftService.getCPGroupPartitionId(t.element1) == partitionId);
    }
}
