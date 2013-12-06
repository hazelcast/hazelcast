package com.hazelcast.replicatedmap.record;

import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

public class ObjectReplicatedRecordStorage<K, V> extends AbstractReplicatedRecordStorage<K, V> {

    public ObjectReplicatedRecordStorage(String name, NodeEngine nodeEngine,
                                         CleanerRegistrator cleanerRegistrator,
                                         ReplicatedMapService replicatedMapService) {
        super(name, nodeEngine, cleanerRegistrator, replicatedMapService);
    }

    @Override
    public Set keySet() {
        return new HashSet(storage.keySet());
    }

    @Override
    public Collection values() {
        List values = new ArrayList(storage.size());
        for (ReplicatedRecord record : storage.values()) {
            values.add(record.getValue());
        }
        return values;
    }

    @Override
    public Set entrySet() {
        Set entrySet = new HashSet(storage.size());
        for (Map.Entry entry : storage.entrySet()) {
            entrySet.add(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
        }
        return entrySet;
    }

}
