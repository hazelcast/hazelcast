package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RaftDataService implements SnapshotAwareService<Map<Long, Object>> {

    public static final String SERVICE_NAME = "RaftTestService";

    private final Map<Long, Object> values = new ConcurrentHashMap<Long, Object>();

    public RaftDataService() {
    }

    Object apply(long commitIndex, Object value) {
        assert !values.containsKey(commitIndex) :
                "Cannot apply " + value + "since commitIndex: " + commitIndex + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(long commitIndex) {
        return values.get(commitIndex);
    }

    public int size() {
        return values.size();
    }

    public Set<Object> values() {
        return new HashSet<Object>(values.values());
    }

    @Override
    public Map<Long, Object> takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        Map<Long, Object> snapshot = new HashMap<Long, Object>();
        for (Entry<Long, Object> e : values.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            snapshot.put(e.getKey(), e.getValue());
        }

        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, Map<Long, Object> snapshot) {
        values.clear();
        values.putAll(snapshot);
    }
}
