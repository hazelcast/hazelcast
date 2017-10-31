package com.hazelcast.raft.impl.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftDataService {

    public static final String SERVICE_NAME = "RaftTestService";

    private final Map<Integer, Object> values = new ConcurrentHashMap<Integer, Object>();

    public RaftDataService() {
    }

    Object apply(int commitIndex, Object value) {
        assert !values.containsKey(commitIndex) :
                "Cannot apply " + value + "since commitIndex: " + commitIndex + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(int commitIndex) {
        return values.get(commitIndex);
    }

    public Set<Object> values() {
        return new HashSet<Object>(values.values());
    }

}
