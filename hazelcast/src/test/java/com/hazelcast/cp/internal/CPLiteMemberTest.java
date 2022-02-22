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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getAddress;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPLiteMemberTest extends HazelcastRaftTestSupport {

    @Test
    public void liteMembers_shouldNotSelectedAsCPMembers_duringInitialDiscovery() throws Exception {
        Config config = createConfig(3, 3);
        Config liteConfig = createConfig(3, 3).setLiteMember(true);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2_lite = factory.newHazelcastInstance(liteConfig);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastInstance hz4_lite = factory.newHazelcastInstance(liteConfig);

        // Discovery cannot be completed,
        // since there are not enough data members.
        assertFalse(awaitUntilDiscoveryCompleted(hz1, 1));
        assertFalse(awaitUntilDiscoveryCompleted(hz2_lite, 1));
        assertFalse(awaitUntilDiscoveryCompleted(hz3, 1));
        assertFalse(awaitUntilDiscoveryCompleted(hz4_lite, 1));

        // Start 3rd data member
        HazelcastInstance hz5 = factory.newHazelcastInstance(config);

        // Discovery can be completed now...
        assertTrue(awaitUntilDiscoveryCompleted(hz1, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz2_lite, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz3, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz4_lite, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz5, 60));

        Collection<CPMember> cpMembers = hz5.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                            .toCompletableFuture().get();
        // Lite members are not part of CP member list
        assertNotCpMember(hz2_lite, cpMembers);
        assertNotCpMember(hz4_lite, cpMembers);
    }

    private void assertNotCpMember(HazelcastInstance hz, Collection<CPMember> cpMembers) {
        for (CPMember member : cpMembers) {
            assertNotEquals(getAddress(hz), member.getAddress());
        }
    }

    private boolean awaitUntilDiscoveryCompleted(HazelcastInstance hz, int seconds) throws InterruptedException {
        return hz.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(seconds, TimeUnit.SECONDS);
    }

    @Test
    public void liteMembers_shouldNotBePromotedToCPMember() throws Exception {
        Config config = createConfig(3, 3);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastInstance hz_lite = factory.newHazelcastInstance(createConfig(3, 3).setLiteMember(true));

        assertTrue(awaitUntilDiscoveryCompleted(hz1, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz_lite, 60));

        try {
            hz_lite.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                   .toCompletableFuture().get();
            fail("CP member promotion should have failed!");
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalStateException.class, e.getCause());
        }

        Collection<CPMember> cpMembers = hz1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                            .toCompletableFuture().get();
        assertEquals(3, cpMembers.size());
        assertNotCpMember(hz_lite, cpMembers);
    }

    @Test
    public void liteMembers_canBePromotedToCPMember_afterPromotedToDataMember() throws Exception {
        Config config = createConfig(3, 3);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastInstance hz_lite = factory.newHazelcastInstance(createConfig(3, 3).setLiteMember(true));

        assertTrue(awaitUntilDiscoveryCompleted(hz1, 60));
        assertTrue(awaitUntilDiscoveryCompleted(hz_lite, 60));

        hz_lite.getCluster().promoteLocalLiteMember();
        hz_lite.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
               .toCompletableFuture().get();

        Collection<CPMember> cpMembers = hz1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                            .toCompletableFuture().get();
        assertEquals(4, cpMembers.size());

        Set<Address> cpAddresses = cpMembers.stream().map(CPMember::getAddress).collect(Collectors.toSet());
        assertThat(cpAddresses, hasItem(getAddress(hz_lite)));
    }


}
