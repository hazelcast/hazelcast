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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhoneHomeCPSubsystemTest extends HazelcastTestSupport {

    private Node node;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        Config config = getConfig();
        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig().setCPMemberCount(3);
        config.setCPSubsystemConfig(cpSubsystemConfig);
        HazelcastInstance hz = createHazelcastInstances(config, 3)[0];
        node = getNode(hz);
        phoneHome = new PhoneHome(node);
    }

    @Test
    public void testCPSubsystemEnabled() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_SUBSYSTEM_ENABLED.getRequestParameterName())).isEqualTo("true");
        assertThat(parameters.get(PhoneHomeMetrics.CP_MEMBERS_COUNT.getRequestParameterName())).isEqualTo("3");
        // no groups are created w/o CP data structures
        assertThat(parameters.get(PhoneHomeMetrics.CP_GROUPS_COUNT.getRequestParameterName())).isEqualTo("0");
    }

    @Test
    public void testGroupsCount_defaultGroups() {
        node.hazelcastInstance.getCPSubsystem().getAtomicLong("hazelcast");

        assertTrueEventually(() -> {
            Map<String, String> parameters = phoneHome.phoneHome(true);

            // two default groups METADATA and DEFAULT
            assertThat(parameters.get(PhoneHomeMetrics.CP_GROUPS_COUNT.getRequestParameterName())).isEqualTo("2");
        });
    }

    @Test
    public void testGroupsCount_createdGroups() {
        node.hazelcastInstance.getCPSubsystem().getSemaphore("hazelcast@firstGroup");
        node.hazelcastInstance.getCPSubsystem().getCountDownLatch("hazelcast@secondGroup");
        node.hazelcastInstance.getCPSubsystem().getLock("hazelcast@firstGroup");
        node.hazelcastInstance.getCPSubsystem().getAtomicLong("hazelcast");
        node.hazelcastInstance.getCPSubsystem().getAtomicReference("hazelcast@thirdGroup");

        assertTrueEventually(() -> {
            Map<String, String> parameters = phoneHome.phoneHome(true);

            // two default groups METADATA and DEFAULT + 3 created
            assertThat(parameters.get(PhoneHomeMetrics.CP_GROUPS_COUNT.getRequestParameterName())).isEqualTo("5");
        });
    }

    @Test
    public void testSemaphoresCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_SEMAPHORES_COUNT.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getCPSubsystem().getSemaphore("hazelcast@firstGroup").init(1);
        node.hazelcastInstance.getCPSubsystem().getSemaphore("hazelcast@secondGroup").init(1);
        node.hazelcastInstance.getCPSubsystem().getSemaphore("hazelcast2@firstGroup").init(1);

        assertTrueEventually(() -> {
            Map<String, String> params = phoneHome.phoneHome(true);
            assertThat(params.get(PhoneHomeMetrics.CP_SEMAPHORES_COUNT.getRequestParameterName())).isEqualTo("3");
        });
    }

    @Test
    public void testFencedLocksCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_FENCED_LOCKS_COUNT.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getCPSubsystem().getLock("hazelcast@firstGroup").lock();
        node.hazelcastInstance.getCPSubsystem().getLock("hazelcast@secondGroup").lock();
        node.hazelcastInstance.getCPSubsystem().getLock("hazelcast2@firstGroup").lock();

        assertTrueEventually(() -> {
            Map<String, String> params = phoneHome.phoneHome(true);
            assertThat(params.get(PhoneHomeMetrics.CP_FENCED_LOCKS_COUNT.getRequestParameterName())).isEqualTo("3");
        });
    }

    @Test
    public void testCountdownLatchesCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_COUNTDOWN_LATCHES_COUNT.getRequestParameterName()))
                .isEqualTo("0");

        node.hazelcastInstance.getCPSubsystem().getCountDownLatch("hazelcast@firstGroup").trySetCount(1);
        node.hazelcastInstance.getCPSubsystem().getCountDownLatch("hazelcast@secondGroup").trySetCount(1);
        node.hazelcastInstance.getCPSubsystem().getCountDownLatch("hazelcast2@firstGroup").trySetCount(1);

        assertTrueEventually(() -> {
            Map<String, String> params = phoneHome.phoneHome(true);
            assertThat(params.get(PhoneHomeMetrics.CP_COUNTDOWN_LATCHES_COUNT.getRequestParameterName()))
                    .isEqualTo("3");
        });
    }

    @Test
    public void testAtomicLongsCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_ATOMIC_LONGS_COUNT.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getCPSubsystem().getAtomicLong("hazelcast@firstGroup").set(42L);
        node.hazelcastInstance.getCPSubsystem().getAtomicLong("hazelcast@secondGroup").set(42L);
        node.hazelcastInstance.getCPSubsystem().getAtomicLong("hazelcast2@firstGroup").set(42L);

        assertTrueEventually(() -> {
            Map<String, String> params = phoneHome.phoneHome(true);
            assertThat(params.get(PhoneHomeMetrics.CP_ATOMIC_LONGS_COUNT.getRequestParameterName())).isEqualTo("3");
        });
    }

    @Test
    public void testAtomicRefsCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertThat(parameters.get(PhoneHomeMetrics.CP_ATOMIC_REFS_COUNT.getRequestParameterName())).isEqualTo("0");

        node.hazelcastInstance.getCPSubsystem().getAtomicReference("hazelcast@firstGroup").set(42L);
        node.hazelcastInstance.getCPSubsystem().getAtomicReference("hazelcast@secondGroup").set(42L);
        node.hazelcastInstance.getCPSubsystem().getAtomicReference("hazelcast2@firstGroup").set(42L);

        assertTrueEventually(() -> {
            Map<String, String> params = phoneHome.phoneHome(true);
            assertThat(params.get(PhoneHomeMetrics.CP_ATOMIC_REFS_COUNT.getRequestParameterName())).isEqualTo("3");
        });
    }

}
