package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.config.raft.RaftAtomicLongConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLongService implements ManagedService, RaftRemoteService, RaftGroupLifecycleAwareService, SnapshotAwareService<RaftAtomicLongSnapshot> {

    public static final String SERVICE_NAME = "hz:raft:atomicLongService";

    private final Map<Tuple2<RaftGroupId, String>, RaftAtomicLong> atomicLongs = new ConcurrentHashMap<Tuple2<RaftGroupId, String>, RaftAtomicLong>();
    private final Set<Tuple2<RaftGroupId, String>> destroyedLongs = newSetFromMap(new ConcurrentHashMap<Tuple2<RaftGroupId, String>, Boolean>());
    private final NodeEngine nodeEngine;
    private volatile RaftService raftService;

    public RaftAtomicLongService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        atomicLongs.clear();
    }

    @Override
    public RaftAtomicLongSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Long> longs = new HashMap<String, Long>();
        for (RaftAtomicLong atomicLong : atomicLongs.values()) {
            if (atomicLong.groupId().equals(groupId)) {
                longs.put(atomicLong.name(), atomicLong.value());
            }
        }

        Set<String> destroyed = new HashSet<String>();
        for (Tuple2<RaftGroupId, String> tuple : destroyedLongs) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return new RaftAtomicLongSnapshot(longs, destroyed);
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, RaftAtomicLongSnapshot snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, Long> e : snapshot.getLongs()) {
            String name = e.getKey();
            long val = e.getValue();
            atomicLongs.put(Tuple2.of(groupId, name), new RaftAtomicLong(groupId, name, val));
        }

        for (String name : snapshot.getDestroyed()) {
            destroyedLongs.add(Tuple2.of(groupId, name));
        }
    }

    @Override
    public IAtomicLong createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            return new RaftAtomicLongProxy(name, groupId, raftService.getInvocationManager());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        destroyedLongs.add(key);
        return atomicLongs.remove(key) != null;
    }

    @Override
    public void onGroupDestroy(RaftGroupId groupId) {
        Iterator<Tuple2<RaftGroupId, String>> iter = atomicLongs.keySet().iterator();
        while (iter.hasNext()) {
            Tuple2<RaftGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                destroyedLongs.add(next);
                iter.remove();
            }
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftAtomicLongConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftAtomicLongConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftAtomicLongConfig(name);
    }

    public RaftAtomicLong getAtomicLong(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        if (destroyedLongs.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicLong[" + name + "] is already destroyed!");
        }
        RaftAtomicLong atomicLong = atomicLongs.get(key);
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(groupId, groupId.name());
            atomicLongs.put(key, atomicLong);
        }
        return atomicLong;
    }
}
