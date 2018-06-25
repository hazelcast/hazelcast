package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftMemberAutoRemoveTest extends HazelcastRaftTestSupport {

    private int missingRaftMemberRemovalSeconds;

    @Test
    public void when_missingCPNodeDoesNotJoin_then_itIsAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 10;
        final HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitUntilCPDiscoveryCompleted(instances);

        final RaftMemberImpl terminatedRaftMember = getRaftService(instances[2]).getLocalMember();
        instances[2].getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<RaftMemberImpl> activeMembers = getRaftService(instances[0]).getMetadataGroupManager()
                                                                                       .getActiveMembers();
                assertFalse(activeMembers.contains(terminatedRaftMember));
                assertTrue(getRaftService(instances[0]).getMissingMembers().isEmpty());
            }
        });
    }

    @Test
    public void when_missingCPNodeJoins_then_itIsNotAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 300;
        final HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitUntilCPDiscoveryCompleted(instances);

        final RaftMemberImpl raftMember0 = getRaftService(instances[0]).getLocalMember();
        final RaftMemberImpl raftMember1 = getRaftService(instances[1]).getLocalMember();
        final RaftMemberImpl raftMember2 = getRaftService(instances[2]).getLocalMember();

        blockCommunicationBetween(instances[1], instances[2]);
        blockCommunicationBetween(instances[0], instances[2]);

        closeConnectionBetween(instances[1], instances[2]);
        closeConnectionBetween(instances[0], instances[2]);

        assertClusterSizeEventually(2, instances[0], instances[1]);
        assertClusterSizeEventually(1, instances[2]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getRaftService(instances[0]).getMissingMembers().contains(raftMember2));
                assertTrue(getRaftService(instances[1]).getMissingMembers().contains(raftMember2));
                assertTrue(getRaftService(instances[2]).getMissingMembers().contains(raftMember0));
                assertTrue(getRaftService(instances[2]).getMissingMembers().contains(raftMember1));
            }
        }, 20);

        unblockCommunicationBetween(instances[1], instances[2]);
        unblockCommunicationBetween(instances[0], instances[2]);

        assertClusterSizeEventually(3, instances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getRaftService(instances[0]).getMissingMembers().isEmpty());
                assertTrue(getRaftService(instances[1]).getMissingMembers().isEmpty());
                assertTrue(getRaftService(instances[2]).getMissingMembers().isEmpty());
            }
        });
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
              .setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5")
              .getRaftConfig()
              .setSessionTimeToLiveSeconds(missingRaftMemberRemovalSeconds)
              .setMissingRaftMemberRemovalSeconds(missingRaftMemberRemovalSeconds);

        return config;
    }

}
