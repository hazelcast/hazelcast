package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftAtomicLongConfig;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.spi.RaftProxyFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockServiceTest extends HazelcastRaftTestSupport {

    private static final String RAFT_GROUP_NAME = "locks";
    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        instances = newInstances(raftGroupSize);
        groupId = getRaftInvocationManager(instances[0]).createRaftGroup(RAFT_GROUP_NAME, raftGroupSize).get();
    }

    @Test
    public void testSnapshotRestore() throws ExecutionException, InterruptedException {
        final HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final HazelcastInstance follower = getRandomFollowerInstance(instances, groupId);

        // the follower falls behind the leader. It neither append entries nor installs snapshots.
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        final ILock lock = RaftProxyFactory.create(leader, RaftLockService.SERVICE_NAME, RAFT_GROUP_NAME);
        lockByOtherThread(lock);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, TimeUnit.MINUTES);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getLockRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getTryLockTimeouts().isEmpty());
            }
        });

        IAtomicLong atomicLong = RaftProxyFactory.create(instances[0], RaftAtomicLongService.SERVICE_NAME, RAFT_GROUP_NAME);
        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            atomicLong.incrementAndGet();
        }

        final RaftNodeImpl leaderRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(leader).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);
        final RaftNodeImpl followerRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(follower).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);

        // the leader takes a snapshot
        final long[] leaderSnapshotIndex = new long[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long idx = getSnapshotEntry(leaderRaftNode).index();
                assertTrue(idx > 0);
                leaderSnapshotIndex[0] = idx;
            }
        });

        // the follower doesn't have it since its raft log is still empty
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getSnapshotEntry(followerRaftNode).index());
            }
        }, 10);

        resetPacketFiltersFrom(leader);

        // the follower installs the snapshot after it hears from the leader
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(leaderSnapshotIndex[0], getSnapshotEntry(followerRaftNode).index());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(follower).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getLockRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getTryLockTimeouts().isEmpty());
            }
        });
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(RAFT_GROUP_NAME, 3));
        config.addRaftLockConfig(new RaftLockConfig(RAFT_GROUP_NAME, RAFT_GROUP_NAME));
        config.addRaftAtomicLongConfig(new RaftAtomicLongConfig(RAFT_GROUP_NAME, RAFT_GROUP_NAME));

        return config;
    }
}
