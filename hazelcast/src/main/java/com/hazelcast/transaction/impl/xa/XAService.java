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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.operations.XaReplicationOperation;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Provides XAResource to the user via proxyService
 * Holds all prepared state xa transactions
 */
public class XAService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:xaService";

    private final NodeEngineImpl nodeEngine;

    private final XAResourceImpl xaResource;

    private final ConcurrentMap<SerializableXID, List<XATransaction>> transactions =
            new ConcurrentHashMap<SerializableXID, List<XATransaction>>();

    public XAService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.xaResource = new XAResourceImpl(nodeEngine, this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        return xaResource;
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
    }

    public TransactionContext newXATransactionContext(Xid xid, UUID ownerUuid, int timeout, boolean originatedFromClient) {
        return new XATransactionContextImpl(nodeEngine, xid, ownerUuid, timeout, originatedFromClient);
    }

    public void putTransaction(XATransaction transaction) {
        SerializableXID xid = transaction.getXid();
        List<XATransaction> list = transactions.get(xid);
        if (list == null) {
            list = new CopyOnWriteArrayList<XATransaction>();
            transactions.put(xid, list);
        }
        list.add(transaction);
    }

    public List<XATransaction> removeTransactions(SerializableXID xid) {
        return transactions.remove(xid);
    }

    public Set<SerializableXID> getPreparedXids() {
        return transactions.keySet();
    }

    //Migration related methods

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        List<XATransactionDTO> migrationData = new ArrayList<XATransactionDTO>();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (Map.Entry<SerializableXID, List<XATransaction>> entry : transactions.entrySet()) {
            SerializableXID xid = entry.getKey();
            int partitionId = partitionService.getPartitionId(xid);
            List<XATransaction> xaTransactionList = entry.getValue();
            for (XATransaction xaTransaction : xaTransactionList) {
                if (partitionId == event.getPartitionId()) {
                    migrationData.add(new XATransactionDTO(xaTransaction));
                }
            }
        }
        if (migrationData.isEmpty()) {
            return null;
        } else {
            return new XaReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    private void clearPartitionReplica(int partitionId) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Iterator<Map.Entry<SerializableXID, List<XATransaction>>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<SerializableXID, List<XATransaction>> entry = iterator.next();
            SerializableXID xid = entry.getKey();
            int xidPartitionId = partitionService.getPartitionId(xid);
            if (xidPartitionId == partitionId) {
                iterator.remove();
            }
        }
    }
}
