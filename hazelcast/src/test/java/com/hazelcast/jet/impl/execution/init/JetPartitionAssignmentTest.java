/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JetPartitionAssignmentTest extends SimpleTestInClusterSupport {
    private static final int MEMBER_COUNT = 7;

    private NodeEngine nodeEngine;
    private List<MemberInfo> members;
    private int partitionCount;

    private Address thisAddress;
    private MemberInfo localMemberInfo;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(MEMBER_COUNT, null);
    }

    @Before
    public void before() throws Exception {
        nodeEngine = getNodeEngineImpl(instance());
        members = Util.getMembersView(nodeEngine).getMembers();
        partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        thisAddress = nodeEngine.getThisAddress();
        localMemberInfo = new MemberInfo((MemberImpl) nodeEngine.getClusterService().getLocalMember());
    }

    @Test
    public void when_noPruning_then_returnsFullAssignment() {
        Map<MemberInfo, int[]> partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(
                nodeEngine,
                members,
                false, null, null, null
        );

        List<Integer> actualAssignedPartitions = partitionAssignment
                .values()
                .parallelStream()
                .flatMap(arr -> Arrays.stream(arr).boxed())
                .sorted()
                .collect(Collectors.toList());

        assertEquals(MEMBER_COUNT, partitionAssignment.size());
        assertEquals(range(0, partitionCount), actualAssignedPartitions);
    }

    @Test
    public void when_requiresOneMemberAndCoordinatorOnly_then_returnsRequiredAssignment() {
        int coordinatorPartition = nodeEngine.getPartitionService().getPartitionId("");
        int requiredNonCoordinatorOwnedPartition = -1;
        Address nonCoordinatorAddress = null;
        for (int i = 0; i < partitionCount; i++) {
            if (!nodeEngine.getPartitionService().getPartitionOwner(i).equals(nodeEngine.getThisAddress())) {
                requiredNonCoordinatorOwnedPartition = i;
                nonCoordinatorAddress = nodeEngine.getPartitionService().getPartitionOwner(i);
                break;
            }
        }

        assertNotNull(nonCoordinatorAddress);
        assertThat(requiredNonCoordinatorOwnedPartition).isGreaterThan(-1);

        Map<MemberInfo, int[]> partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(
                nodeEngine,
                members,
                false,
                Collections.singleton(requiredNonCoordinatorOwnedPartition),
                Set.of(coordinatorPartition),
                Collections.singleton(localMemberInfo.getAddress())
        );

        List<Integer> actualAssignedPartitions = partitionAssignment
                .values()
                .parallelStream()
                .flatMap(arr -> Arrays.stream(arr).boxed())
                .sorted()
                .collect(Collectors.toList());

        MemberInfo requiredMemberInfo = new MemberInfo(nodeEngine.getClusterService().getMember(nonCoordinatorAddress));
        assertThat(partitionAssignment.keySet()).contains(localMemberInfo, requiredMemberInfo);

        assertThat(actualAssignedPartitions)
                .containsExactly(requiredNonCoordinatorOwnedPartition, coordinatorPartition);
    }

    @Test
    public void when_requiresAllPartitions_then_returnsCompressedAssignment() {
        int requiredCoordinatorOwnedPartition = -1;
        for (int i = 0; i < partitionCount; i++) {
            Address thisAddress = nodeEngine.getThisAddress();
            if (nodeEngine.getPartitionService().getPartitionOwner(i).equals(thisAddress)) {
                requiredCoordinatorOwnedPartition = i;
                break;
            }
        }

        assertThat(requiredCoordinatorOwnedPartition).isGreaterThan(-1);

        Map<MemberInfo, int[]> partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(
                nodeEngine,
                members,
                true,
                Collections.singleton(requiredCoordinatorOwnedPartition),
                Set.of(),
                Set.of()
        );

        List<Integer> actualAssignedPartitions = partitionAssignment
                .values()
                .parallelStream()
                .flatMap(arr -> Arrays.stream(arr).boxed())
                .sorted()
                .collect(Collectors.toList());

        assertThat(partitionAssignment.keySet()).contains(localMemberInfo);
        assertThat(actualAssignedPartitions).containsAll(range(0, partitionCount));
    }

    @Test
    public void when_requiresAllPartitionsAndCoordinatorRequired_then_returnsCompressedAssignment() {
        int requiredNonCoordinatorOwnedPartition = -1;
        int requiredCoordinatorOwnedPartition = -1;
        Address nonCoordinatorAddress = null;

        for (int i = 0; i < partitionCount; i++) {
            if (nodeEngine.getPartitionService().getPartitionOwner(i).equals(thisAddress)
                    && requiredCoordinatorOwnedPartition < 0) {
                requiredCoordinatorOwnedPartition = i;
            }
            if (!nodeEngine.getPartitionService().getPartitionOwner(i).equals(nodeEngine.getThisAddress())
                    && requiredNonCoordinatorOwnedPartition < 0) {
                requiredNonCoordinatorOwnedPartition = i;
                nonCoordinatorAddress = nodeEngine.getPartitionService().getPartitionOwner(i);
            }

            if (requiredCoordinatorOwnedPartition > 0 && requiredNonCoordinatorOwnedPartition > 0) {
                break;
            }
        }

        assertThat(nonCoordinatorAddress).isNotNull();

        MemberInfo requiredMemberInfo = new MemberInfo(nodeEngine.getClusterService().getMember(nonCoordinatorAddress));

        Map<MemberInfo, int[]> partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(
                nodeEngine,
                members,
                true,
                Set.of(requiredNonCoordinatorOwnedPartition, requiredCoordinatorOwnedPartition),
                Set.of(),
                Set.of(localMemberInfo.getAddress())
        );

        List<Integer> actualAssignedPartitions = partitionAssignment
                .values()
                .parallelStream()
                .flatMap(arr -> Arrays.stream(arr).boxed())
                .sorted()
                .collect(Collectors.toList());

        assertThat(partitionAssignment.keySet()).contains(localMemberInfo, requiredMemberInfo);
        assertThat(actualAssignedPartitions).containsAll(range(0, partitionCount));
    }
}
