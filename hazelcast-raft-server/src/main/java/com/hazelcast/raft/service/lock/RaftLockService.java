package com.hazelcast.raft.service.lock;

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService implements ManagedService, SnapshotAwareService {

    public static final String SERVICE_NAME = "hz:raft:lockService";

    private final ConcurrentMap<Tuple2<RaftGroupId, String>, RaftLock> locks = new ConcurrentHashMap<Tuple2<RaftGroupId, String>, RaftLock>();
    private volatile NodeEngine nodeEngine;
    private volatile RaftService raftService;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public Object takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        // TODO fixit
        return null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, Object snapshot) {
        // TODO fixit
    }

    public ILock createNew(String name, String uuid) throws ExecutionException, InterruptedException {
        RaftGroupId groupId = createNewAsync(name).get();
        return new RaftLockProxy(name, groupId, raftService.getInvocationManager(), uuid);
    }

    public ICompletableFuture<RaftGroupId> createNewAsync(String name) {
        RaftLockConfig config = getConfig(name);
        checkNotNull(config);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupConfig groupConfig = config.getRaftGroupConfig();
        if (groupConfig != null) {
            return invocationManager.createRaftGroup(groupConfig);
        } else {
            return invocationManager.createRaftGroup(config.getRaftGroupRef());
        }
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    public RaftLockProxy newProxy(String name, RaftGroupId groupId, String uid) {
        return new RaftLockProxy(name, groupId, raftService.getInvocationManager(), uid);
    }

    private RaftLock getRaftLock(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        RaftLock raftLock = locks.get(key);
        if (raftLock == null) {
            raftLock = new RaftLock(groupId, name);
            locks.put(key, raftLock);
        }
        return raftLock;
    }

    public boolean acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invUid, boolean wait) {
        RaftLock raftLock = getRaftLock(groupId, name);
        return raftLock.acquire(endpoint, commitIndex, invUid, wait);
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invUid) {
        RaftLock raftLock = getRaftLock(groupId, name);
        Collection<Long> indices = raftLock.release(endpoint, invUid);
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, true);
            }
        }
    }

    public Tuple2<LockEndpoint, Integer> lockCount(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        RaftLock raftLock = locks.get(Tuple2.of(groupId, name));
        if (raftLock == null) {
            return Tuple2.of(null, 0);
        }
        return raftLock.lockCount();
    }
}
