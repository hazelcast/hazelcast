package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLongService implements ManagedService, SnapshotAwareService<Long> {

    public static final String SERVICE_NAME = "hz:raft:atomicLongService";
    public static final String PREFIX = "atomiclong:";

    private final Map<RaftGroupId, RaftAtomicLong> map = new ConcurrentHashMap<RaftGroupId, RaftAtomicLong>();
    private volatile RaftService raftService;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public Long takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        RaftAtomicLong atomicLong = map.get(raftGroupId);
        if (atomicLong == null) {
            throw new IllegalArgumentException("Unknown raftGroupId -> " + raftGroupId);
        }
        assert atomicLong.commitIndex() == commitIndex : "Value: " + atomicLong + ", Commit-Index: " + commitIndex;
        return atomicLong.value();
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, Long snapshot) {
        RaftAtomicLong atomicLong = new RaftAtomicLong(raftGroupId.name(), snapshot, commitIndex);
        map.put(raftGroupId, atomicLong);
    }

    public static String nameWithoutPrefix(String raftName) {
        assert raftName.startsWith(PREFIX) : "Raft-Name: " + raftName;
        return raftName.substring(PREFIX.length());
    }

    // TODO: in config, nodeCount or failure tolerance ?
    public IAtomicLong createNew(String name, int nodeCount) throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupId groupId = invocationManager.createRaftGroup(SERVICE_NAME, PREFIX + name, nodeCount);
        return new RaftAtomicLongProxy(groupId, invocationManager);
    }

    public IAtomicLong newProxy(RaftGroupId groupId) {
        return new RaftAtomicLongProxy(groupId, raftService.getInvocationManager());
    }

    public RaftAtomicLong getAtomicLong(RaftGroupId groupId) {
        RaftAtomicLong atomicLong = map.get(groupId);
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(groupId.name());
            map.put(groupId, atomicLong);
        }
        return atomicLong;
    }
}
