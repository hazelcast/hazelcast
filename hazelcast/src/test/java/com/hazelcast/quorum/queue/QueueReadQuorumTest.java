/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.queue;

import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueueReadQuorumTest extends AbstractQueueQuorumTest {

    @BeforeClass
    public static void initialize() throws Exception {
        initializeFiveMemberCluster(QuorumType.READ, 3);
        addQueueData(q4);
        cluster.splitFiveMembersThreeAndTwo();
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMutatingOperationsSuccessfulWhenQuorumSizeNotMet() throws Exception {
        q4.put("foo");
        q4.offer("foo");
        q4.add("foo");
        q4.take();
        q4.remove();
        q4.poll();
    }

    @Test(expected = QuorumException.class)
    public void testElementOperationThrowsExceptionWhenQuorumSizeNotMet() {
        q4.element();
    }

    @Test(expected = QuorumException.class)
    public void testPeekOperationThrowsExceptionWhenQuorumSizeNotMet() {
        q4.peek();
    }

    @Test
    public void testGetLocalQueueStatsOperationSuccessfulWhenQuorumSizeNotMet() {
        q4.getLocalQueueStats();
    }
}
