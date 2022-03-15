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

package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Includes tests for node and cluster safety.
 * <p>
 * TODO: tests are not sufficient -> add more tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SafeClusterTest extends HazelcastTestSupport {

    @Test
    public void isClusterSafe() {
        HazelcastInstance node = createHazelcastInstance();
        boolean safe = node.getPartitionService().isClusterSafe();

        assertTrue(safe);
    }

    @Test
    public void isClusterSafe_multiNode() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance();
        HazelcastInstance node2 = factory.newHazelcastInstance();
        boolean safe1 = node1.getPartitionService().isClusterSafe();
        boolean safe2 = node2.getPartitionService().isClusterSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isLocalMemberSafe() {
        HazelcastInstance node = createHazelcastInstance();
        boolean safe = node.getPartitionService().isLocalMemberSafe();

        assertTrue(safe);
    }

    @Test
    public void isLocalMemberSafe_multiNode() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance();
        HazelcastInstance node2 = factory.newHazelcastInstance();
        boolean safe1 = node1.getPartitionService().isLocalMemberSafe();
        boolean safe2 = node2.getPartitionService().isLocalMemberSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isMemberSafe_localMember() {
        HazelcastInstance node = createHazelcastInstance();
        Member localMember = node.getCluster().getLocalMember();
        boolean safe = node.getPartitionService().isMemberSafe(localMember);

        assertTrue(safe);
    }

    @Test
    public void test_forceLocalMemberToBeSafe() {
        HazelcastInstance node = createHazelcastInstance();
        boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(5, TimeUnit.SECONDS);

        assertTrue(safe);
    }
}
