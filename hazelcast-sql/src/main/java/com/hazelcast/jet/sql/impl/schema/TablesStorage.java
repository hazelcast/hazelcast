/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.GetOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.view.View;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class TablesStorage {
    private static final int MAX_CHECK_ATTEMPTS = 5;
    private static final long SLEEP_MILLIS = 100;

    private static final String CATALOG_MAP_NAME = "__sql.catalog";

    private final NodeEngine nodeEngine;
    private final Object mergingMutex = new Object();
    private final ILogger logger;

    private volatile boolean storageMovedToNew;

    public TablesStorage(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    void put(String name, Mapping mapping) {
        newStorage().put(name, mapping);
        if (!useOnlyNewStorage()) {
            oldStorage().put(name, mapping);
            awaitMappingOnAllMembers(name, mapping);
        }
    }

    void put(String name, View view) {
        newStorage().put(name, view);
        if (!useOnlyNewStorage()) {
            oldStorage().put(name, view);
            awaitMappingOnAllMembers(name, view);
        }
    }

    boolean putIfAbsent(String name, Mapping mapping) {
        Object previousNew = newStorage().putIfAbsent(name, mapping);
        Object previousOld = null;
        if (!useOnlyNewStorage()) {
            previousOld = oldStorage().putIfAbsent(name, mapping);
        }
        return previousNew == null && previousOld == null;
    }

    boolean putIfAbsent(String name, View view) {
        Object previousNew = newStorage().putIfAbsent(name, view);
        Object previousOld = null;
        if (!useOnlyNewStorage()) {
            previousOld = oldStorage().putIfAbsent(name, view);
        }
        return previousNew == null && previousOld == null;
    }

    Mapping removeMapping(String name) {
        Mapping removedNew = (Mapping) newStorage().remove(name);
        Mapping removedOld = null;
        if (!useOnlyNewStorage()) {
            removedOld = (Mapping) oldStorage().remove(name);
        }
        return removedNew == null ? removedOld : removedNew;
    }

    View getView(String name) {
        Object obj = mergedStorage().get(name);
        if (obj instanceof View) {
            return (View) obj;
        }
        return null;
    }

    View removeView(String name) {
        View removedNew = (View) newStorage().remove(name);
        View removedOld = null;
        if (!useOnlyNewStorage()) {
            removedOld = (View) oldStorage().remove(name);
        }
        return removedNew == null ? removedOld : removedNew;
    }

    Collection<Object> allObjects() {
        return mergedStorage().values();
    }

    Collection<String> mappingNames() {
        return mergedStorage().values()
                .stream()
                .filter(m -> m instanceof Mapping)
                .map(m -> ((Mapping) m).name())
                .collect(Collectors.toList());
    }

    Collection<String> viewNames() {
        return mergedStorage().values()
                .stream()
                .filter(v -> v instanceof View)
                .map(v -> ((View) v).name())
                .collect(Collectors.toList());
    }

    void registerListener(EntryListener<String, Object> listener) {
        if (!nodeEngine.getLocalMember().isLiteMember()) {
            newStorage().addEntryListener(listener, false);
            oldStorage().addEntryListener(listener);
        }
    }

    ReplicatedMap<String, Object> oldStorage() {
        // To remove in 5.3. We are using the old storage if the cluster version is lower than 5.2. We destroy the old catalog
        // during the first read after the cluster version is upgraded to > =5.2. We do not synchronize put operations to the old
        // catalog, so it is possible that destroy happens before put, and we end up with a leaked ReplicatedMap.
        return nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);
    }

    IMap<String, Object> newStorage() {
        return nodeEngine.getHazelcastInstance().getMap(CATALOG_MAP_NAME);
    }

    private Map<String, Object> mergedStorage() {
        IMap<String, Object> newStorage = newStorage();
        if (useOnlyNewStorage()) {
            if (!storageMovedToNew) {
                synchronized (mergingMutex) {
                    if (!storageMovedToNew) {
                        ReplicatedMap<String, Object> oldStorage = oldStorage();
                        oldStorage.forEach(newStorage::putIfAbsent);
                        oldStorage.destroy();
                        storageMovedToNew = true;
                    }
                }
            }
            return newStorage;
        } else {
            Map<String, Object> mergedCatalog = new HashMap<>();
            mergedCatalog.putAll(oldStorage());
            mergedCatalog.putAll(newStorage);
            return mergedCatalog;
        }
    }

    private boolean useOnlyNewStorage() {
        return nodeEngine.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V5_2);
    }

    private Collection<Address> getMemberAddresses() {
        return nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).stream()
                .filter(member -> !member.localMember() && !member.isLiteMember())
                .map(Member::getAddress)
                .collect(toSet());
    }

    /**
     * Temporary measure to ensure schema is propagated to all the members.
     */
    @SuppressWarnings("BusyWait")
    private void awaitMappingOnAllMembers(String name, IdentifiedDataSerializable metadata) {
        Data keyData = nodeEngine.getSerializationService().toData(name);
        int keyPartitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
        OperationService operationService = nodeEngine.getOperationService();

        Collection<Address> memberAddresses = getMemberAddresses();
        for (int i = 0; i < MAX_CHECK_ATTEMPTS && !memberAddresses.isEmpty(); i++) {
            List<CompletableFuture<Address>> futures = memberAddresses.stream()
                    .map(memberAddress -> {
                        Operation operation = new GetOperation(CATALOG_MAP_NAME, keyData)
                                .setPartitionId(keyPartitionId)
                                .setValidateTarget(false);
                        return operationService
                                .createInvocationBuilder(ReplicatedMapService.SERVICE_NAME, operation, memberAddress)
                                .setTryCount(1)
                                .invoke()
                                .toCompletableFuture()
                                .thenApply(result -> Objects.equals(metadata, result) ? memberAddress : null);
                    }).collect(toList());
            for (CompletableFuture<Address> future : futures) {
                try {
                    memberAddresses.remove(future.join());
                } catch (Exception e) {
                    logger.warning("Error occurred while trying to fetch mapping: " + e.getMessage(), e);
                }
            }
            if (!memberAddresses.isEmpty()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    abstract static class EntryListenerAdapter implements EntryListener<String, Object> {

        @Override
        public final void entryAdded(EntryEvent<String, Object> event) {
        }

        @Override
        public abstract void entryUpdated(EntryEvent<String, Object> event);

        @Override
        public abstract void entryRemoved(EntryEvent<String, Object> event);

        @Override
        public final void entryEvicted(EntryEvent<String, Object> event) {
            throw new UnsupportedOperationException("SQL catalog entries must never be evicted - " + event);
        }

        @Override
        public void entryExpired(EntryEvent<String, Object> event) {
            throw new UnsupportedOperationException("SQL catalog entries must never be expired - " + event);
        }

        @Override
        public final void mapCleared(MapEvent event) {
            throw new UnsupportedOperationException("SQL catalog must never be cleared - " + event);
        }

        @Override
        public final void mapEvicted(MapEvent event) {
            throw new UnsupportedOperationException("SQL catalog must never be evicted - " + event);
        }
    }
}
