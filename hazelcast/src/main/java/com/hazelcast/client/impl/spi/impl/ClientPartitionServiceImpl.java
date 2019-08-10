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

import com.hazelcast.client.impl.ClientPartitionListenerService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostListener;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;
    private static final long BLOCKING_GET_ONCE_SLEEP_MILLIS = 100;
    private final BiConsumer<ClientMessage, Throwable> refreshTaskCallback = new RefreshTaskCallback();
    private final ClientExecutionServiceImpl clientExecutionService;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final AtomicReference<PartitionTable> partitionTable =
            new AtomicReference<>(new PartitionTable(null, -1, new Int2ObjectHashMap<>()));
    private volatile int partitionCount;
    private volatile long lastCorrelationId = -1;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
        clientExecutionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
    }

    public void start() {
        //scheduling left in place to support server versions before 3.9.
        clientExecutionService.scheduleWithRepetition(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
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

    public void listenPartitionTable(Connection connection) throws Exception {
        //when we connect to cluster back we need to reset partition state version
        //we are keeping the partition map as is because, user may want its operations run on connected members even if
        //owner connection is gone, and partition table is missing.
        // See @{link ClientProperty#ALLOW_INVOCATIONS_WHEN_DISCONNECTED}
        Int2ObjectHashMap<Address> partitions = getPartitions();
        partitionTable.set(new PartitionTable(connection, -1, partitions));

        //Servers after 3.9 supports listeners
        ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, connection);
        invocation.setEventHandler(new PartitionEventHandler(connection));
        invocation.invokeUrgent().get();
        lastCorrelationId = clientMessage.getCorrelationId();
    }

    public void cleanupOnDisconnect() {
        ((AbstractClientListenerService) client.getListenerService()).removeEventHandler(lastCorrelationId);
    }

    void refreshPartitions() {
        try {
            // use internal execution service for all partition refresh process (do not use the user executor thread)
            clientExecutionService.execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            ignore(ignored);
        }
    }

    private void waitForPartitionCountSetOnce() {
        while (partitionCount == 0 && client.getConnectionManager().isAlive()) {
            ClientClusterService clusterService = client.getClientClusterService();
            Collection<Member> memberList = clusterService.getMemberList();
            Connection currentOwnerConnection = this.partitionTable.get().connection;
            //if member list is empty or owner is null, we will sleep and retry
            if (memberList.isEmpty() || currentOwnerConnection == null) {
                sleepBeforeNextTry();
                continue;
            }
            if (isClusterFormedByOnlyLiteMembers(memberList)) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }

            ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
            // invocation should go to owner connection because the listener is added to owner connection
            // and partition state version should be fetched from one member to keep it consistent
            ClientInvocationFuture future =
                    new ClientInvocation(client, requestMessage, null, currentOwnerConnection).invokeUrgent();
            try {
                ClientMessage responseMessage = future.get();
                ClientGetPartitionsCodec.ResponseParameters response =
                        ClientGetPartitionsCodec.decodeResponse(responseMessage);
                Connection connection = responseMessage.getConnection();
                processPartitionResponse(connection, response.partitions, response.partitionStateVersion);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private void sleepBeforeNextTry() {
        try {
            Thread.sleep(BLOCKING_GET_ONCE_SLEEP_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private boolean isClusterFormedByOnlyLiteMembers(Collection<Member> memberList) {
        return MemberSelectingCollection.count(memberList, MemberSelectors.DATA_MEMBER_SELECTOR) == 0;
    }

    /**
     * The partitions can be empty on the response, client will not apply the empty partition table,
     * see {@link ClientPartitionListenerService#getPartitions(PartitionTableView)}
     */
    private void processPartitionResponse(Connection connection, Collection<Map.Entry<Address, List<Integer>>> partitions,
                                          int partitionStateVersion) {

        while (true) {
            PartitionTable current = this.partitionTable.get();
            if (!shouldBeApplied(connection, partitions, partitionStateVersion, current)) {
                return;
            }
            Int2ObjectHashMap<Address> newPartitions = convertToPartitionToAddressMap(partitions);
            PartitionTable newMetaData = new PartitionTable(connection, partitionStateVersion, newPartitions);
            if (this.partitionTable.compareAndSet(current, newMetaData)) {
                // partition count is set once at the start. Even if we reset the partition table when switching cluster
                //we want to remember the partition count. That is why it is a different field.
                if (partitionCount == 0) {
                    partitionCount = newPartitions.size();
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Processed partition response. partitionStateVersion : " + partitionStateVersion
                            + ", partitionCount :" + newPartitions.size() + ", connection : " + connection);
                }
                return;
            }

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
        if (partitionCount == 0) {
            waitForPartitionCountSetOnce();
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

    private final class PartitionEventHandler extends ClientAddPartitionListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final Connection clientConnection;

        private PartitionEventHandler(Connection clientConnection) {
            this.clientConnection = clientConnection;
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }

        @Override
        public void handlePartitionsEvent(Collection<Map.Entry<Address, List<Integer>>> partitions, int partitionStateVersion) {
            processPartitionResponse(clientConnection, partitions, partitionStateVersion);
        }
    }

    private final class RefreshTask implements Runnable {

        private RefreshTask() {
        }

        @Override
        public void run() {
            try {
                Connection connection = ClientPartitionServiceImpl.this.partitionTable.get().connection;
                if (connection == null) {
                    return;
                }
                ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
                ClientInvocationFuture future =
                        new ClientInvocation(client, requestMessage, null, connection).invokeUrgent();
                future.whenCompleteAsync(refreshTaskCallback);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private class RefreshTaskCallback implements BiConsumer<ClientMessage, Throwable> {

        @Override
        public void accept(ClientMessage responseMessage, Throwable throwable) {
            if (throwable == null) {
                if (responseMessage == null) {
                    return;
                }
                Connection connection = responseMessage.getConnection();
                ClientGetPartitionsCodec.ResponseParameters response = ClientGetPartitionsCodec.decodeResponse(responseMessage);
                processPartitionResponse(connection, response.partitions, response.partitionStateVersion);
            } else {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", throwable);
                }
            }
        }
    }
}
