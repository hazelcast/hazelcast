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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides XAResource to the user via proxyService
 * Holds all prepared state xa transactions
 */
public class XAService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:xaService";

    private final NodeEngineImpl nodeEngine;

    private final XAResourceImpl xaResource;

    private final ConcurrentMap<SerializableXID, List<XATransactionImpl>> preparedTransactions =
            new ConcurrentHashMap<SerializableXID, List<XATransactionImpl>>();

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
    public DistributedObject createDistributedObject(String objectName) {
        return xaResource;
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }

    public TransactionContext newXaTransactionContext(Xid xid, int timeout) {
        return new XATransactionContextImpl(nodeEngine, xid, null, timeout);
    }

    public void putTransaction(XATransactionImpl transaction) {
        SerializableXID xid = transaction.getXid();
        List<XATransactionImpl> list = preparedTransactions.get(xid);
        if (list == null) {
            list = new ArrayList<XATransactionImpl>();
            preparedTransactions.put(xid, list);
        }
        // this method is always called from same thread for same xid, so no concurrency problem
        list.add(transaction);
    }

    public List<XATransactionImpl> removeTransactions(SerializableXID xid) {
        return preparedTransactions.remove(xid);
    }

    public Set<SerializableXID> getXids() {
        return preparedTransactions.keySet();
    }

    //Migration related methods

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        List<XATransactionHolder> migrationData = new ArrayList<XATransactionHolder>();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (Map.Entry<SerializableXID, List<XATransactionImpl>> entry : preparedTransactions.entrySet()) {
            SerializableXID xid = entry.getKey();
            int partitionId = partitionService.getPartitionId(xid);
            List<XATransactionImpl> xaTransactionList = entry.getValue();
            for (XATransactionImpl xaTransaction : xaTransactionList) {
                if (partitionId == event.getPartitionId()
                        && event.getReplicaIndex() <= 1) {
                    migrationData.add(new XATransactionHolder(xaTransaction));
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
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Iterator<Map.Entry<SerializableXID, List<XATransactionImpl>>> iterator = preparedTransactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<SerializableXID, List<XATransactionImpl>> entry = iterator.next();
            SerializableXID xid = entry.getKey();
            int xidPartitionId = partitionService.getPartitionId(xid);
            if (xidPartitionId == partitionId) {
                iterator.remove();
            }
        }
    }
}
