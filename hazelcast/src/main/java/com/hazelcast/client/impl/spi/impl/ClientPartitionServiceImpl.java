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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostListener;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final long BLOCKING_GET_ONCE_SLEEP_MILLIS = 100;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final AtomicReference<PartitionTable> partitionTable =
            new AtomicReference<>(new PartitionTable(null, -1, new Int2ObjectHashMap<>()));
    private volatile int partitionCount;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
    }

    public void start() {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (ListenerConfig listenerConfig : listenerConfigs) {
                EventListener implementation = listenerConfig.getImplementation();
                if (implementation == null) {
                    try {
                        implementation = ClassLoaderUtil.newInstance(classLoader, listenerConfig.getClassName());
                    } catch (Exception e) {
                        logger.severe(e);
                    }
                }

                if (implementation instanceof PartitionLostListener) {
                    client.getPartitionService().addPartitionLostListener((PartitionLostListener) implementation);
                }
            }
        }
    }

    private static class PartitionTable {
        final Connection connection;
        final int partitionSateVersion;
        final Int2ObjectHashMap<Address> partitions;

        PartitionTable(Connection connection, int partitionSateVersion, Int2ObjectHashMap<Address> partitions) {
            this.connection = connection;
            this.partitionSateVersion = partitionSateVersion;
            this.partitions = partitions;
        }
    }


    /**
     * The partitions can be empty on the response, client will not apply the empty partition table,
     */
    public void handlePartitionsViewEvent(Connection connection, Collection<Map.Entry<Address, List<Integer>>> partitions,
                                          int partitionStateVersion) {
        if (logger.isFinestEnabled()) {
            logger.finest("Handling new partition table with  partitionStateVersion: " + partitionStateVersion);
        }
        while (true) {
            PartitionTable current = this.partitionTable.get();
            if (!shouldBeApplied(connection, partitions, partitionStateVersion, current)) {
                return;
            }
            Int2ObjectHashMap<Address> newPartitions = convertToPartitionToAddressMap(partitions);
            PartitionTable newMetaData = new PartitionTable(connection, partitionStateVersion, newPartitions);
            if (this.partitionTable.compareAndSet(current, newMetaData)) {
                onPartitionTableUpdate();
                return;
            }

        }
    }

    private void onPartitionTableUpdate() {
        //  partition count is set once at the start. Even if we reset the partition table when switching cluster
        // we want to remember the partition count. That is why it is a different field.
        PartitionTable partitionTable = this.partitionTable.get();
        Int2ObjectHashMap<Address> newPartitions = partitionTable.partitions;
        if (partitionCount == 0) {
            partitionCount = newPartitions.size();
        }
        if (logger.isFineEnabled()) {
            logger.fine("Processed partition response. partitionStateVersion : " + partitionTable.partitionSateVersion
                    + ", partitionCount :" + newPartitions.size());
        }
    }

    private boolean shouldBeApplied(Connection connection, Collection<Map.Entry<Address, List<Integer>>> partitions,
                                    int partitionStateVersion, PartitionTable current) {
        if (partitions.isEmpty()) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, current,
                        "response is empty");
            }
            return false;
        }
        if (!connection.equals(current.connection)) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, current,
                        "response is from old connection");
            }
            return false;
        }
        if (partitionStateVersion <= current.partitionSateVersion) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, current,
                        "response state version is old");
            }
            return false;
        }
        return true;
    }

    private void logFailure(Connection connection, int partitionStateVersion, PartitionTable current, String cause) {

        logger.finest(" We will not apply the response, since " + cause + " . Response is from " + connection
                + ". Current connection " + current.connection
                + " response state version:" + partitionStateVersion
                + ". Current state version: " + current.partitionSateVersion);
    }

    private Int2ObjectHashMap<Address> convertToPartitionToAddressMap(Collection<Map.Entry<Address, List<Integer>>> partitions) {
        Int2ObjectHashMap<Address> newPartitions = new Int2ObjectHashMap<Address>();
        for (Map.Entry<Address, List<Integer>> entry : partitions) {
            Address address = entry.getKey();
            for (Integer partition : entry.getValue()) {
                newPartitions.put(partition, address);
            }
        }
        return newPartitions;
    }

    public void reset() {
        partitionTable.set(new PartitionTable(null, -1, new Int2ObjectHashMap<Address>()));
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        return getPartitions().get(partitionId);
    }

    private Int2ObjectHashMap<Address> getPartitions() {
        return partitionTable.get().partitions;
    }

    @Override
    public int getPartitionId(@Nonnull Data key) {
        final int pc = getPartitionCount();
        if (pc <= 0) {
            return 0;
        }
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, pc);
    }

    @Override
    public int getPartitionId(@Nonnull Object key) {
        final Data data = client.getSerializationService().toData(key);
        return getPartitionId(data);
    }

    @Override
    public int getPartitionCount() {
        while (partitionCount == 0 && client.getConnectionManager().isAlive()) {
            ClientClusterService clusterService = client.getClientClusterService();
            Collection<Member> memberList = clusterService.getMemberList();
            if (MemberSelectingCollection.count(memberList, MemberSelectors.DATA_MEMBER_SELECTOR) == 0) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }
            try {
                Thread.sleep(BLOCKING_GET_ONCE_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ExceptionUtil.rethrow(e);
            }
        }
        return partitionCount;
    }

    @Override
    public Partition getPartition(int partitionId) {
        return new PartitionImpl(partitionId);
    }

    private final class PartitionImpl implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            Address owner = getPartitionOwner(partitionId);
            if (owner != null) {
                return client.getClientClusterService().getMember(owner);
            }
            return null;
        }

        @Override
        public String toString() {
            return "PartitionImpl{partitionId=" + partitionId + '}';
        }
    }
}
