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

package com.hazelcast.cp.internal.datastructures.spi.atomic;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hazelcast.cp.internal.RaftService.getCPGroupPartitionId;
import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

/**
 * Contains Raft-based atomic value instances, implements snapshotting,
 * and creates proxies
 */
public abstract class RaftAtomicValueService<T, V extends RaftAtomicValue<T>, S extends RaftAtomicValueSnapshot<T>>
        extends AbstractCPMigrationAwareService
        implements RaftManagedService, RaftRemoteService, RaftNodeLifecycleAwareService, SnapshotAwareService<S> {

    private static final UUID ZERO_UUID = new UUID(0, 0);
    protected final Map<BiTuple<CPGroupId, String>, V> atomicValues = new ConcurrentHashMap<>();
    private final Set<BiTuple<CPGroupId, String>> destroyedValues = newSetFromMap(new ConcurrentHashMap<>());
    private volatile RaftService raftService;

    public RaftAtomicValueService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {
        if (!raftService.isCpSubsystemEnabled()) {
            clearValues();
        }
    }

    private void clearValues() {
        atomicValues.clear();
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
    public final S takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, T> values = new HashMap<>();
        for (V value : atomicValues.values()) {
            if (value.groupId().equals(groupId)) {
                values.put(getCombinedObjectName(value.name(), value.uuid()), value.get());
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

    protected abstract S newSnapshot(Map<String, T> values, Set<String> destroyed);

    @Override
    public final void restoreSnapshot(CPGroupId groupId, long commitIndex, S snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, T> e : snapshot.getValues()) {
            String name = e.getKey();
            T val = e.getValue();
            UUID uuid = getUuidFromName(name).orElse(ZERO_UUID);
            String objectName = getObjectName(name);
            atomicValues.put(BiTuple.of(groupId, objectName), newAtomicValue(groupId, objectName, val, uuid));
        }

        for (String name : snapshot.getDestroyed()) {
            destroyedValues.add(BiTuple.of(groupId, name));
        }
    }

    protected abstract V newAtomicValue(CPGroupId groupId, String name, T val, UUID uuid);

    @Override
    public final void onRaftNodeTerminated(CPGroupId groupId) {
        Iterator<BiTuple<CPGroupId, String>> iter = atomicValues.keySet().iterator();
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
        V atomicValue = getAtomicValue(groupId, name);
        System.out.println("Destroying: " + atomicValue);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, atomicValue.name());
        destroyedValues.add(key);
        return atomicValues.remove(key) != null;
    }

    public int getAtomicValuesCount() {
        return atomicValues.size();
    }

    public final V getAtomicValue(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        System.out.println("getAtomicValueName: " + name);
        Optional<UUID> uuid = getUuidFromName(name);
        String objectName = getObjectName(name);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, objectName);
        // Commented to check destroy-recreate object cycle
//        if (destroyedValues.contains(key)) {
//            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
//        }
        V atomicValue = atomicValues.get(key);
        System.out.println("getAtomicValue: " + atomicValue);
        // For 5.3 compatibility
        if (atomicValue == null && uuid.isEmpty()) {
            atomicValue = newAtomicValue(groupId, name, null, ZERO_UUID);
            System.out.println("Create " + key + ": " + atomicValue);
            atomicValues.put(key, atomicValue);
        } else if (atomicValue == null || (uuid.isPresent() && !atomicValue.uuid().equals(uuid.get()))) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        return atomicValue;
    }

    public final UUID createOrGetRaftObject(CPGroupId groupId, String objectName, UUID uuid) {
        checkNotNull(groupId);
        checkNotNull(objectName);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, objectName);
        // Commented to check destroy-recreate object cycle
//        if (destroyedValues.contains(key)) {
//            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
//        }
        V atomicValue = atomicValues.get(key);
        if (atomicValue == null) {
            atomicValue = newAtomicValue(groupId, objectName, null, uuid);
            atomicValues.put(key, atomicValue);
            System.out.println("Init " + key + ": " + atomicValue);
        }
        return atomicValue.uuid();
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

    protected abstract DistributedObject newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId,
            String proxyName, String objectNameForProxy);

    @Override
    protected int getBackupCount() {
        return 1;
    }

    @Override
    protected final Map<CPGroupId, Object> getSnapshotMap(int partitionId) {
        assert !raftService.isCpSubsystemEnabled();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return atomicValues.keySet().stream()
                .filter(tuple -> getCPGroupPartitionId(tuple.element1, partitionCount) == partitionId)
                .map(tuple -> tuple.element1)
                .distinct()
                .map(groupId -> BiTuple.of(groupId, takeSnapshot(groupId, 0L)))
                .collect(Collectors.toMap(tuple -> tuple.element1, tuple -> tuple.element2));
    }

    @Override
    protected final void clearPartitionReplica(int partitionId) {
        atomicValues.keySet().removeIf(t -> raftService.getCPGroupPartitionId(t.element1) == partitionId);
    }

    private static Optional<UUID> getUuidFromName(String name) {
        int index = name.indexOf("@");
        if (index > 0) {
            return Optional.of(UUID.fromString(name.substring(index + 1)));
        } else {
            return Optional.empty();
        }
    }

    private static String getObjectName(String name) {
        return name.indexOf("@") > 0 ? name.substring(0, name.indexOf("@")).trim() : name;
    }

    private String getCombinedObjectName(String objectName, UUID objectUUID) {
        return objectName + "@" + objectUUID;
    }
}
