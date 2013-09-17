package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.txn.TransactionalSetProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.transaction.impl.TransactionSupport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 9/3/13
 */
public class SetService extends CollectionService {

    public static final String SERVICE_NAME = "hz:impl:setService";

    private final ConcurrentMap<String, SetContainer> containerMap = new ConcurrentHashMap<String, SetContainer>();

    public SetService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    public SetContainer getOrCreateContainer(String name, boolean backup) {
        SetContainer container = containerMap.get(name);
        if (container == null){
            container = new SetContainer(name, nodeEngine, this);
            final SetContainer current = containerMap.putIfAbsent(name, container);
            if (current != null){
                container = current;
            }
        }
        return container;
    }

    public Map<String, ? extends CollectionContainer> getContainerMap() {
        return containerMap;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new SetProxyImpl(String.valueOf(objectId), nodeEngine, this);
    }

    public TransactionalSetProxy createTransactionalObject(Object id, TransactionSupport transaction) {
        return new TransactionalSetProxy(String.valueOf(id), transaction, nodeEngine, this);
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final Map<String, CollectionContainer> migrationData = getMigrationData(event);
        return migrationData.isEmpty() ? null : new SetReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }
}
