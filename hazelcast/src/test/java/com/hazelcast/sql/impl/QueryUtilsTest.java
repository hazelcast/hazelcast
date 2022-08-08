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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryUtilsTest extends CoreSqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testVersionMismatch() {
        HazelcastInstance member = factory.newHazelcastInstance();

        NodeEngine nodeEngine = Accessors.getNodeEngineImpl(member);
        String memberId = nodeEngine.getLocalMember().getUuid().toString();
        String memberVersion = nodeEngine.getLocalMember().getVersion().toString();

        try {
            QueryUtils.createPartitionMap(nodeEngine, new MemberVersion(0, 0, 0), false);

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertEquals("Cannot execute SQL query when members have different versions (make sure that all members "
                + "have the same version) {localMemberId=" + memberId + ", localMemberVersion=0.0.0, remoteMemberId="
                + memberId + ", remoteMemberVersion=" + memberVersion + "}", e.getMessage());
        }
    }

    @Test
    public void testUnassignedPartition_ignore() {
        HazelcastInstance member = factory.newHazelcastInstance();

        member.getCluster().changeClusterState(ClusterState.FROZEN);

        Map<UUID, PartitionIdSet> map = QueryUtils.createPartitionMap(Accessors.getNodeEngineImpl(member), null, false);

        assertTrue(map.isEmpty());
    }

    @Test
    public void testUnassignedPartition_exception() {
        HazelcastInstance member = factory.newHazelcastInstance();

        member.getCluster().changeClusterState(ClusterState.FROZEN);

        try {
            QueryUtils.createPartitionMap(Accessors.getNodeEngineImpl(member), null, true);

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.PARTITION_DISTRIBUTION, e.getCode());
            assertTrue(e.getMessage(), e.getMessage().startsWith("Partition is not assigned to any member"));
        }
    }

    @Test
    // the test calls methods that return random value - let's try multiple times so that it doesn't pass by chance
    @Repeat(5)
    public void test_findLightJobCoordinator() {
        MemberVersion v1 = MemberVersion.of(0, 1, 0);
        MemberVersion v2 = MemberVersion.of(0, 2, 0);
        MemberVersion v3 = MemberVersion.of(0, 3, 0);

        Member mv1 = mock(Member.class);
        when(mv1.getVersion()).thenReturn(v1);

        Member mv1_1 = mock(Member.class);
        when(mv1_1.getVersion()).thenReturn(v1);

        Member mv1_lite = mock(Member.class);
        when(mv1_lite.getVersion()).thenReturn(v1);
        when(mv1_lite.isLiteMember()).thenReturn(true);

        Member mv2 = mock(Member.class);
        when(mv2.getVersion()).thenReturn(v2);

        Member mv2_1 = mock(Member.class);
        when(mv2_1.getVersion()).thenReturn(v2);

        Member mv2_lite = mock(Member.class);
        when(mv2_lite.getVersion()).thenReturn(v2);
        when(mv2_lite.isLiteMember()).thenReturn(true);

        Member mv3 = mock(Member.class);
        when(mv3.getVersion()).thenReturn(v3);

        // failing cases
        assertNull(QueryUtils.memberOfLargerSameVersionGroup(emptyList(), null));
        assertNull(QueryUtils.memberOfLargerSameVersionGroup(singletonList(mv1_lite), null));
        assertNull(QueryUtils.memberOfLargerSameVersionGroup(singletonList(mv1_lite), mv1_lite));
        assertThatThrownBy(() -> QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv2, mv3), null))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("More than 2 distinct member versions found: 0.1, 0.2, 0.3");

        // one member in the cluster
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(singletonList(mv1), mv1));

        // two members with same ver - must choose the local one
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1), mv1));
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1_1, mv1), mv1));

        // lite and non-lite same ver, must choose non-lite
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_lite), mv1));
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_lite), mv1_lite));

        // two versions, same count - must choose newer even if not local
        assertSame(mv2, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv2), mv1));
        assertSame(mv2, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv2), mv2));

        // older version has bigger group - must use local
        assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1, mv2), mv1));

        // older group bigger, but all lite - choose newer
        assertSame(mv2, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1_lite, mv1_lite, mv2), null));

        // test cases with random result
        assertTrueEventuallyFast(() -> assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1), null)));
        assertTrueEventuallyFast(() -> assertSame(mv1_1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1), null)));
        assertTrueEventuallyFast(() -> assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1), mv2)));
        assertTrueEventuallyFast(() -> assertSame(mv1_1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1), mv2)));

        // older version group larger - must choose one of the older
        assertTrueEventuallyFast(() -> assertSame(mv1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1, mv2), mv2)));
        assertTrueEventuallyFast(() -> assertSame(mv1_1, QueryUtils.memberOfLargerSameVersionGroup(asList(mv1, mv1_1, mv2), mv2)));
    }

    private static void assertTrueEventuallyFast(Runnable r) {
        for (int i = 0; i < 1000; i++) {
            try {
                r.run();
                return;
            } catch (AssertionError ignored) {
            }
        }
        fail("tried 1000 times and failed");
    }
}
