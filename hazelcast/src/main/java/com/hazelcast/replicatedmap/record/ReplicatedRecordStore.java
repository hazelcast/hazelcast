package com.hazelcast.replicatedmap.record;

import com.hazelcast.replicatedmap.ReplicatedMapService;

import java.util.Collection;
import java.util.Set;

public interface ReplicatedRecordStore {

    String getName();

    Object remove(Object key);

    Object get(Object key);

    Object put(Object key, Object value);

    boolean containsKey(Object key);

    boolean containsValue(Object value);

    ReplicatedRecord getReplicatedRecord(Object key);

    ReplicatedRecord putReplicatedRecord(Object key, ReplicatedRecord replicatedRecord);

    Set keySet();

    Collection values();

    Set entrySet();

    int size();

    void clear();

    boolean isEmpty();

    ReplicatedMapService getReplicatedMapService();

    void destroy();

}
