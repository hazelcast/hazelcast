/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMemberAutoRemoveTest extends HazelcastRaftTestSupport {

    private int missingRaftMemberRemovalSeconds;

    @Test
    public void when_missingCPNodeDoesNotJoin_then_itIsAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 10;
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        CPMemberInfo terminatedMember = (CPMemberInfo) instances[2].getCPSubsystem().getLocalCPMember();
        instances[2].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            Collection<CPMemberInfo> activeMembers = getRaftService(instances[0]).getMetadataGroupManager()
                                                                                 .getActiveMembers();
            assertThat(activeMembers, not(hasItem(terminatedMember)));
            assertThat(getRaftService(instances[0]).getMissingMembers(), Matchers.empty());
        });
    }

    @Test
    public void when_missingCPNodeJoins_then_itIsNotAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 300;
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        CPMember cpMember0 = instances[0].getCPSubsystem().getLocalCPMember();
        CPMember cpMember1 = instances[1].getCPSubsystem().getLocalCPMember();
        CPMember cpMember2 = instances[2].getCPSubsystem().getLocalCPMember();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertEquals(3, getRaftService(instance).getMetadataGroupManager().getActiveMembers().size());
            }
        });

        blockCommunicationBetween(instances[1], instances[2]);
        blockCommunicationBetween(instances[0], instances[2]);

        closeConnectionBetween(instances[1], instances[2]);
        closeConnectionBetween(instances[0], instances[2]);

        assertClusterSizeEventually(2, instances[0], instances[1]);
        assertClusterSizeEventually(1, instances[2]);

        assertTrueEventually(() -> {
            assertThat(getRaftService(instances[0]).getMissingMembers(), hasItem((CPMemberInfo) cpMember2));
            assertThat(getRaftService(instances[1]).getMissingMembers(), hasItem((CPMemberInfo) cpMember2));
            assertThat(getRaftService(instances[2]).getMissingMembers(), hasItem((CPMemberInfo) cpMember0));
            assertThat(getRaftService(instances[2]).getMissingMembers(), hasItem((CPMemberInfo) cpMember1));
        });

        unblockCommunicationBetween(instances[1], instances[2]);
        unblockCommunicationBetween(instances[0], instances[2]);

        assertClusterSizeEventually(3, instances);

        assertTrueEventually(() -> {
            assertThat(getRaftService(instances[0]).getMissingMembers(), Matchers.empty());
            assertThat(getRaftService(instances[1]).getMissingMembers(), Matchers.empty());
            assertThat(getRaftService(instances[2]).getMissingMembers(), Matchers.empty());
        });
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
              .setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5")
              .getCPSubsystemConfig()
              .setSessionTimeToLiveSeconds(missingRaftMemberRemovalSeconds)
              .setMissingCPMemberAutoRemovalSeconds(missingRaftMemberRemovalSeconds);

        return config;
    }

}
