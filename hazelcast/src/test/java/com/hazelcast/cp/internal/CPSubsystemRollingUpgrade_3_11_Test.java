/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import com.hazelcast.version.Version;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.instance.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestHazelcastInstanceFactory.initOrCreateConfig;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

// RU_COMPAT_3_11
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPSubsystemRollingUpgrade_3_11_Test extends HazelcastRaftTestSupport {

    @Rule
    public final OverridePropertyRule version_3_11_rule = set(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_11.toString());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void whenGetCPSubsystem_onVersion_3_11_thenFailsWithUnsupportedOperationException() {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        expectedException.expect(UnsupportedOperationException.class);
        instance.getCPSubsystem();
    }

    @Test
    public void whenGetCPSubsystem_afterVersionUpgrade_thenShouldSuccess() {
        HazelcastInstance instance = newUpgradableHazelcastInstance(createConfig(3, 3));
        instance.getCluster().changeClusterVersion(Versions.V3_12);
        instance.getCPSubsystem();
    }

    @Test
    public void whenCPSubsystemEnabled_onVersion_3_11_thenCPDiscoveryShouldNotComplete() {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = factory.newHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getRaftService(instance);
                    MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
                    assertNull(metadataGroupManager.getLocalCPMember());
                    assertFalse(metadataGroupManager.isDiscoveryCompleted());
                    assertThat(metadataGroupManager.getActiveMembers(), Matchers.<CPMemberInfo>empty());
                }
            }
        }, 5);
    }

    @Test
    public void whenCPSubsystemEnabled_afterVersion_thenCPDiscoveryShouldComplete() {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = newUpgradableHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        instances[0].getCluster().changeClusterVersion(Versions.V3_12);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getRaftService(instance);
                    MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
                    assertTrue(metadataGroupManager.isDiscoveryCompleted());
                    assertNotNull(metadataGroupManager.getLocalCPMember());
                    assertThat(metadataGroupManager.getActiveMembers(), not(Matchers.<CPMemberInfo>empty()));
                }
            }
        });
    }

    @Test
    public void whenCreateRaftGroupCalled_onVersion_3_11_thenFailsWithUnsupportedOperationException() {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = factory.newHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        RaftService service = getRaftService(instances[0]);
        expectedException.expect(UnsupportedOperationException.class);
        service.getInvocationManager().createRaftGroup("default", 3).join();
    }

    @Test
    public void whenCreateRaftGroupCalled_afterUpgrade_thenShouldSuccess() throws Exception {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = newUpgradableHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        instances[0].getCluster().changeClusterVersion(Versions.V3_12);

        RaftService service = getRaftService(instances[0]);
        service.getInvocationManager().createRaftGroup("default", 3).get();
    }

    @Test
    public void whenCPDataStructureCreated_onVersion_3_11_thenFailsWithUnsupportedOperationException() {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = factory.newHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        RaftAtomicLongService service = getNodeEngineImpl(instances[0]).getService(RaftAtomicLongService.SERVICE_NAME);
        expectedException.expect(UnsupportedOperationException.class);
        service.createProxy("atomic");
    }

    @Test
    public void whenCPDataStructureCreated_afterUpgrade_thenShouldSuccess() {
        int clusterSize = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = newUpgradableHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        instances[0].getCluster().changeClusterVersion(Versions.V3_12);
        instances[0].getCPSubsystem().getAtomicLong("atomic").get();
    }

    private HazelcastInstance newUpgradableHazelcastInstance(Config config) {
        return newHazelcastInstance(initOrCreateConfig(config), randomName(), new CustomNodeContext());
    }

    private class CustomNodeContext extends MockNodeContext {
        CustomNodeContext() {
            super(factory.getRegistry(), factory.nextAddress());
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new AlwaysCompatibleNodeExtension(node);
        }
    }

    private static class AlwaysCompatibleNodeExtension extends DefaultNodeExtension {
        AlwaysCompatibleNodeExtension(Node node) {
            super(node);
        }

        @Override
        public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
            return true;
        }
    }
}
