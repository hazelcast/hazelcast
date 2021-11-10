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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapEvent;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.GetOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Mapping;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class AbstractMetadataStorage<T> {

    protected static final int MAX_CHECK_ATTEMPTS = 5;
    protected static final long SLEEP_MILLIS = 100;

    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final String catalogName;

    public AbstractMetadataStorage(NodeEngine nodeEngine, String catalogName) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.catalogName = catalogName;
    }

    void put(String name, T data) {
        checkMappingPresence(name);
        storage().put(name, data);
        awaitMappingOnAllMembers(name, data);
    }

    boolean putIfAbsent(String name, T data) {
        Object previous = storage().putIfAbsent(name, data);
        awaitMappingOnAllMembers(name, data);
        return previous == null;
    }

    @SuppressWarnings("unchecked")
    T remove(String name) {
        return (T) storage().remove(name);
    }

    abstract void registerListener(EntryListener<String, Object> listener);

    protected ReplicatedMap<String, Object> storage() {
        return nodeEngine.getHazelcastInstance().getReplicatedMap(catalogName);
    }

    protected Collection<Address> getMemberAddresses() {
        return nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).stream()
                .filter(member -> !member.localMember() && !member.isLiteMember())
                .map(Member::getAddress)
                .collect(toSet());
    }

    /**
     * Temporary measure to ensure schema is propagated to all the members.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitMappingOnAllMembers(String name, T metadata) {
        Data keyData = nodeEngine.getSerializationService().toData(name);
        int keyPartitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
        OperationService operationService = nodeEngine.getOperationService();

        Collection<Address> memberAddresses = getMemberAddresses();
        for (int i = 0; i < MAX_CHECK_ATTEMPTS && !memberAddresses.isEmpty(); i++) {
            List<CompletableFuture<Address>> futures = memberAddresses.stream()
                    .map(memberAddress -> {
                        Operation operation = new GetOperation(catalogName, keyData)
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

    /**
     * Since mappings and views sharing the same ReplicatedMap, we should detect name collisions.
     */
    private void checkMappingPresence(String name) {
        if (storage().containsKey(name) && storage().get(name) instanceof Mapping) {
            throw QueryException.error("SQL catalog contains mapping with name '" + name + "'." +
                    " Try to choose another view name");
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
