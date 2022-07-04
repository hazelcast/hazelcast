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

package com.hazelcast.jet.impl.execution.init;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionArrangementTest {

    private Address a0;
    private Address a1;
    private Address a2;
    private PartitionArrangement a;

    @Before
    public void before() throws Exception {
        a0 = new Address("1.2.3.4", 0);
        a1 = new Address("1.2.3.4", 1);
        a2 = new Address("1.2.3.4", 2);

        a = new PartitionArrangement(
                ImmutableMap.of(
                        a0, new int[]{0, 3, 6},
                        a1, new int[]{2, 4},
                        a2, new int[]{1, 5}),
                a0);
    }

    @Test
    public void testRemotePartitionAssignment() {
        assertEquals(2, a.getRemotePartitionAssignment().size()); // only remote members are included
        assertArrayEquals(new int[]{2, 4}, a.getRemotePartitionAssignment().get(a1));
        assertArrayEquals(new int[]{1, 5}, a.getRemotePartitionAssignment().get(a2));
    }

    @Test
    public void testAssignPartitionsToProcessors() {
        assertArrayEquals(
                new int[][]{new int[]{0, 1, 2, 3, 4, 5, 6}},
                a.assignPartitionsToProcessors(1, false));

        assertArrayEquals(
                new int[][]{
                        new int[]{0, 2, 4, 6},
                        new int[]{1, 3, 5}},
                a.assignPartitionsToProcessors(2, false));

        assertArrayEquals(
                new int[][]{
                        new int[]{0, 3, 6},
                        new int[]{1, 4},
                        new int[]{2, 5}
                },
                a.assignPartitionsToProcessors(3, false));

        assertArrayEquals(
                new int[][]{new int[]{0, 3, 6}},
                a.assignPartitionsToProcessors(1, true));

        assertArrayEquals(
                new int[][]{
                        new int[]{0, 6},
                        new int[]{3}},
                a.assignPartitionsToProcessors(2, true));

        assertArrayEquals(
                new int[][]{
                        new int[]{0},
                        new int[]{3},
                        new int[]{6}},
                a.assignPartitionsToProcessors(3, true));
    }
}
