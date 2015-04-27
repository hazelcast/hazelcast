/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddPartitionLostListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.PartitionLostEventParameters;
import com.hazelcast.client.impl.protocol.parameters.RemovePartitionLostListenerParameters;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 5/16/13
 */
public final class PartitionServiceProxy implements PartitionService {

    private final ClientPartitionService partitionService;
    private final ClientListenerService listenerService;
    private final Random random = new Random();

    public PartitionServiceProxy(ClientPartitionService partitionService, ClientListenerService listenerService) {
        this.partitionService = partitionService;
        this.listenerService = listenerService;
    }

    @Override
    public String randomPartitionKey() {
        return Integer.toString(random.nextInt(partitionService.getPartitionCount()));
    }

    @Override
    public Set<Partition> getPartitions() {
        final int partitionCount = partitionService.getPartitionCount();
        Set<Partition> partitions = new LinkedHashSet<Partition>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            final Partition partition = partitionService.getPartition(i);
            partitions.add(partition);
        }
        return partitions;
    }

    @Override
    public Partition getPartition(Object key) {
        final int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartition(partitionId);
    }

    @Override
    public String addMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener partitionLostListener) {
        ClientMessage request = AddPartitionLostListenerParameters.encode();
        final EventHandler<ClientMessage> handler = new ClientPartitionLostEventHandler(partitionLostListener);
        return listenerService.startListening(request, null, handler);
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        ClientMessage request = RemovePartitionLostListenerParameters.encode(registrationId);
        return listenerService.stopListening(request, registrationId);
    }

    @Override
    public boolean isClusterSafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMemberSafe(Member member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocalMemberSafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    private static class ClientPartitionLostEventHandler implements EventHandler<ClientMessage> {

        private PartitionLostListener listener;

        public ClientPartitionLostEventHandler(PartitionLostListener listener) {
            this.listener = listener;
        }

        @Override
        public void handle(ClientMessage clientMessage) {
            PartitionLostEventParameters event = PartitionLostEventParameters.decode(clientMessage);
            listener.partitionLost(new PartitionLostEvent(event.partitionId, event.lostBackupCount, event.source));
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }
    }

}
