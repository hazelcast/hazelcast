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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionServiceLiteMemberTest extends HazelcastTestSupport {

    private final Config liteMemberConfig = new Config().setLiteMember(true);

    /**
     * PARTITION ASSIGNMENT
     **/

    @Test
    public void test_partitionsNotAssigned_withLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        partitionService.firstArrangement();

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertNull(partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    @Test
    public void test_partitionsAreAssigned_afterDataMemberJoins() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance liteInstance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(liteInstance);
        partitionService.firstArrangement();

        final HazelcastInstance dataInstance = factory.newHazelcastInstance();
        warmUpPartitions(liteInstance, dataInstance);

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertEquals(getNode(dataInstance).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    /**
     * GET PARTITION
     **/

    @Test
    public void test_getPartition_nullPartitionOwnerOnMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        assertNull(partitionService.getPartition(0).getOwnerOrNull());
    }

    @Test
    public void test_getPartition_nullPartitionOwnerOnNonMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        for (HazelcastInstance instance : asList(master, other)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);

            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                assertNull(partitionService.getPartition(0).getOwnerOrNull());
            }
        }
    }

    @Test
    public void test_getPartition_afterLiteMemberLeavesTheCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance dataInstance = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        warmUpPartitions(dataInstance, lite);

        lite.getLifecycleService().shutdown();

        assertClusterSizeEventually(1, dataInstance);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(dataInstance);

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertEquals(getNode(dataInstance).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    @Test
    public void test_getPartition_afterDataMemberLeavesTheCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance dataInstance = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        warmUpPartitions(lite);

        dataInstance.getLifecycleService().shutdown();

        for (HazelcastInstance instance : asList(master, lite)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    for (int i = 0; i < partitionService.getPartitionCount(); i++) {
                        assertEquals(getNode(master).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
                    }
                }
            });
        }
    }

    /**
     * GET PARTITION OWNER
     **/

    @Test
    public void test_getPartitionOwner_onMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test
    public void test_getPartitionOwner_onNonMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        for (HazelcastInstance instance : asList(master, other)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
            assertNull(partitionService.getPartitionOwner(0));
        }
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getPartitionOwnerOrWait_onMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getPartitionOwnerOrWait_onNonMasterLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(other);
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_getPartitionOwnerOrWait_onLiteMemberAfterDataMemberTerminates() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                try {
                    final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
                    partitionService.getPartitionOwnerOrWait(0);
                    fail();
                } catch (NoDataMemberInClusterException expected) {
                    ignore(expected);
                }
            }
        });
    }

    @Test
    public void test_getPartitionOwnerOrWait_onLiteMemberAfterDataMemberShutsDown() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, master, lite);
        warmUpPartitions(master, lite);

        master.getLifecycleService().shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                try {
                    final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
                    partitionService.getPartitionOwnerOrWait(0);
                    fail();
                } catch (NoDataMemberInClusterException expected) {
                    ignore(expected);
                }
            }
        });
    }

    /**
     * GRACEFUL SHUTDOWN
     **/

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_withClusterSize1() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        lite.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_withClusterSize2() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance(liteMemberConfig);
        lite1.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_whenDataMemberExistsInCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        warmUpPartitions(lite, other);

        lite.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanShutdownSafely_withClusterSize1() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance master = factory.newHazelcastInstance();

        master.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanShutdownSafely_whenOnlyLiteMemberExistsInCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().shutdown();
    }

    /**
     * TERMINATE
     **/

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_withClusterSize1() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        lite.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_withClusterSize2() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, lite1, lite2);

        lite1.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_whenDataMemberExistsInCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        warmUpPartitions(lite, other);

        lite.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanTerminate_withClusterSize1() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance master = factory.newHazelcastInstance();

        master.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanTerminate_whenOnlyLiteMemberExistsInCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().terminate();
    }

    /**
     * GET MEMBER PARTITIONS MAP
     **/

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getMemberPartitionsMap_withOnlyLiteMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
        partitionService.getMemberPartitionsMap();
    }

    @Test
    public void test_getMemberPartitionsMap_withLiteAndDataMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance dataInstance = factory.newHazelcastInstance();

        warmUpPartitions(lite, dataInstance);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
        final Map<Address, List<Integer>> partitionsMap = partitionService.getMemberPartitionsMap();

        assertEquals(1, partitionsMap.size());
        final List<Integer> partitions = partitionsMap.get(getAddress(dataInstance));
        assertNotNull(partitions);
        assertFalse(partitions.isEmpty());
    }

    /**
     * MEMBER GROUP SIZE
     **/

    @Test
    public void test_memberGroupSize_withSingleLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        assertMemberGroupsSizeEventually(lite, 0);
    }

    @Test
    public void test_memberGroupSize_withMultipleLiteMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, lite, lite2);

        for (HazelcastInstance instance : asList(lite, lite2)) {
            assertMemberGroupsSizeEventually(instance, 0);
        }
    }

    @Test
    public void test_memberGroupSize_withOneLiteMemberAndOneDataMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        assertClusterSize(2, lite, other);

        for (HazelcastInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }
    }

    @Test
    public void test_memberGroupSize_afterDataMemberLeavesTheCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        assertClusterSize(2, lite, other);

        for (HazelcastInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }

        other.getLifecycleService().shutdown();
        assertClusterSizeEventually(1, lite);
        assertMemberGroupsSizeEventually(lite, 0);
    }

    @Test
    public void test_memberGroupSize_afterLiteMemberLeavesTheCluster() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        assertClusterSize(2, lite, other);

        for (HazelcastInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }

        lite.getLifecycleService().shutdown();
        assertClusterSizeEventually(1, other);
        assertMemberGroupsSizeEventually(other, 1);
    }

    private void assertMemberGroupsSizeEventually(final HazelcastInstance instance, final int memberGroupSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
                assertEquals(memberGroupSize, partitionService.getMemberGroupsSize());
            }
        });
    }

    /**
     * MAX BACKUP COUNT
     **/

    @Test
    public void test_maxBackupCount_withSingleLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);

        assertMaxBackupCountEventually(lite, 0);
    }

    @Test
    public void test_maxBackupCount_withTwoLiteMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteMemberConfig);

        assertClusterSize(2, lite, lite2);

        for (HazelcastInstance instance : asList(lite, lite2)) {
            assertMaxBackupCountEventually(instance, 0);
        }
    }

    @Test
    public void test_maxBackupCount_withOneLiteMemberAndOneDataMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();

        assertClusterSize(2, lite, other);

        for (HazelcastInstance instance : asList(lite, other)) {
            assertMaxBackupCountEventually(instance, 0);
        }
    }

    @Test
    public void test_maxBackupCount_withOneLiteMemberAndTwoDataMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance other = factory.newHazelcastInstance();
        final HazelcastInstance other2 = factory.newHazelcastInstance();

        assertClusterSize(3, lite, other2);
        assertClusterSizeEventually(3, other);

        for (HazelcastInstance instance : asList(lite, other, other2)) {
            assertMaxBackupCountEventually(instance, 1);
        }
    }

    private void assertMaxBackupCountEventually(final HazelcastInstance instance, final int maxBackupCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
                assertEquals(maxBackupCount, partitionService.getMaxAllowedBackupCount());
            }
        });
    }

    private InternalPartitionServiceImpl getInternalPartitionServiceImpl(HazelcastInstance instance) {
        return (InternalPartitionServiceImpl) getNode(instance).getPartitionService();
    }
}
