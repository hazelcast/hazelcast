package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class HyperLogLogService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:hyperLogLogService";

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, HyperLogLogContainer> containers =
            new ConcurrentHashMap<String, HyperLogLogContainer>();
    private final ConstructorFunction<String, HyperLogLogContainer> hyperLogLogContainerConstructorFunction =
            new ConstructorFunction<String, HyperLogLogContainer>() {
                @Override
                public HyperLogLogContainer createNew(String arg) {
                    return new HyperLogLogContainer();
                }
            };

    public HyperLogLogContainer getHyperLogLogContainer(String name) {
        return getOrPutIfAbsent(containers, name, hyperLogLogContainerConstructorFunction);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public HyperLogLogProxy createDistributedObject(String objectName) {
        return new HyperLogLogProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        containers.remove(objectName);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return null;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        //TODO @tkountis
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        //TODO @tkountis
    }

}
