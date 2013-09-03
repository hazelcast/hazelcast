package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 9/3/13
 */
public class SetService extends CollectionService {

    public static final String SERVICE_NAME = "hz:impl:listService";

    private final ConcurrentMap<String, SetContainer> containerMap = new ConcurrentHashMap<String, SetContainer>();

    public SetService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    public CollectionContainer getOrCreateContainer(String name, boolean backup) {
        SetContainer container = containerMap.get(name);
        if (container == null){
            container = new SetContainer();
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
}
