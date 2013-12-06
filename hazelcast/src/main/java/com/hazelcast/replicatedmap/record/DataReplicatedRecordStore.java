package com.hazelcast.replicatedmap.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

public class DataReplicatedRecordStore extends AbstractReplicatedRecordStorage<Data, Data> {

    private final NodeEngine nodeEngine;

    public DataReplicatedRecordStore(String name, NodeEngine nodeEngine,
                                     CleanerRegistrator cleanerRegistrator,
                                     ReplicatedMapService replicatedMapService) {
        super(name, nodeEngine, cleanerRegistrator, replicatedMapService);
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Object get(Object key) {
        return nodeEngine.toObject(super.get(nodeEngine.toData(key)));
    }

    @Override
    public Object put(Object key, Object value) {
        return nodeEngine.toObject(super.put(nodeEngine.toData(key), nodeEngine.toData(value)));
    }

    @Override
    public Set keySet() {
        Set keySet = new HashSet();
        for (Data keyData : storage.keySet()) {
            keySet.add(nodeEngine.toObject(keyData));
        }
        return keySet;
    }

    public Collection values() {
        List values = new ArrayList();
        for (ReplicatedRecord record : storage.values()) {
            values.add(nodeEngine.toObject(record.getValue()));
        }
        return values;
    }

    @Override
    public Set entrySet() {
        Set entrySet = new HashSet(storage.size());
        for (Map.Entry entry : storage.entrySet()) {
            entrySet.add(new AbstractMap.SimpleEntry(
                    nodeEngine.toObject(entry.getKey()), nodeEngine.toObject(entry.getValue())));
        }
        return entrySet;
    }

    @Override
    public Object remove(Object key) {
        return nodeEngine.toObject(super.remove(nodeEngine.toData(key)));
    }

}
