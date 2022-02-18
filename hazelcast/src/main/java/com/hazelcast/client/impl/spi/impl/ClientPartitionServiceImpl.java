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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientTriggerPartitionAssignmentCodec;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.Partition;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final AtomicReference<PartitionTable> partitionTable =
            new AtomicReference<>(new PartitionTable(null, -1, new Int2ObjectHashMap<>()));
    private final AtomicInteger partitionCount = new AtomicInteger(0);

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
    }

    private static class PartitionTable {
        final Connection connection;
        final int partitionSateVersion;
        final Int2ObjectHashMap<UUID> partitions;

        PartitionTable(Connection connection, int partitionSateVersion, Int2ObjectHashMap<UUID> partitions) {
            this.connection = connection;
            this.partitionSateVersion = partitionSateVersion;
            this.partitions = partitions;
        }
    }

    /**
     * The partitions can be empty on the response, client will not apply the empty partition table,
     */
    public void handlePartitionsViewEvent(Connection connection, Collection<Map.Entry<UUID, List<Integer>>> partitions,
                                          int partitionStateVersion) {
        if (logger.isFinestEnabled()) {
            logger.finest("Handling new partition table with  partitionStateVersion: " + partitionStateVersion);
        }
        while (true) {
            PartitionTable current = this.partitionTable.get();
            if (!shouldBeApplied(connection, partitions, partitionStateVersion, current)) {
                return;
            }
            Int2ObjectHashMap<UUID> newPartitions = convertToMap(partitions);
            PartitionTable newMetaData = new PartitionTable(connection, partitionStateVersion, newPartitions);
            if (this.partitionTable.compareAndSet(current, newMetaData)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Applied partition table with partitionStateVersion : " + partitionStateVersion);
                }
                return;
            }

        }
    }

    private boolean shouldBeApplied(Connection connection, Collection<Map.Entry<UUID, List<Integer>>> partitions,
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
                logger.finest("Event coming from a new connection. Old connection: " + current.connection
                        + ", new connection " + connection);
            }
            return true;
        }
        if (partitionStateVersion <= current.partitionSateVersion) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, current, "response partition state version is old");
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

    private Int2ObjectHashMap<UUID> convertToMap(Collection<Map.Entry<UUID, List<Integer>>> partitions) {
        Int2ObjectHashMap<UUID> newPartitions = new Int2ObjectHashMap<>();
        for (Map.Entry<UUID, List<Integer>> entry : partitions) {
            UUID uuid = entry.getKey();
            for (Integer partition : entry.getValue()) {
                newPartitions.put(partition, uuid);
            }
        }
        return newPartitions;
    }

    public void reset() {
        partitionTable.set(new PartitionTable(null, -1, new Int2ObjectHashMap<>()));
    }

    @Override
    public UUID getPartitionOwner(int partitionId) {
        return getPartitions().get(partitionId);
    }

    private Int2ObjectHashMap<UUID> getPartitions() {
        return partitionTable.get().partitions;
    }

    @Override
    public int getPartitionId(@Nonnull Data key) {
        int pc = getPartitionCount();
        if (pc == 0) {
            //Partition count can not be zero for the sync mode.
            // On the sync mode, we are waiting for the first connection to be established.
            // We are initializing the partition count with the value coming from the server with authentication.
            // This exception is used only for async mode client.
            throw new HazelcastClientOfflineException();
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
        return partitionCount.get();
    }

    @Override
    public Partition getPartition(int partitionId) {
        return new PartitionImpl(partitionId);
    }

    /**
     * @param newPartitionCount
     * @return true if partition count can be set for the first time, or it is equal to one that is already available,
     * returns false otherwise
     */
    public boolean checkAndSetPartitionCount(int newPartitionCount) {
        if (partitionCount.compareAndSet(0, newPartitionCount)) {
            return true;
        }
        return partitionCount.get() == newPartitionCount;
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
            UUID owner = getPartitionOwner(partitionId);
            if (owner == null) {
                ClientMessage message = ClientTriggerPartitionAssignmentCodec.encodeRequest();
                ClientInvocation invocation = new ClientInvocation(client, message, null);
                invocation.invoke();
                return null;
            }
            return client.getClientClusterService().getMember(owner);
        }

        @Override
        public String toString() {
            return "PartitionImpl{partitionId=" + partitionId + '}';
        }
    }
}
