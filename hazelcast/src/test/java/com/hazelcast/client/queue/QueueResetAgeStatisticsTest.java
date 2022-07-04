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

package com.hazelcast.client.queue;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCResetQueueAgeStatisticsCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueResetAgeStatisticsTest
        extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastClientInstanceImpl client;
    private HazelcastInstance member;

    @Before
    public void setup() {
        member = hazelcastFactory.newHazelcastInstance();
        client = ((HazelcastClientProxy) hazelcastFactory.newHazelcastClient()).client;
    }

    @Test
    public void testAgeStatsReset()
            throws ExecutionException, InterruptedException {
        IQueue<String> myQueue = client.getQueue("my-queue");
        assertEquals(Long.MAX_VALUE, member.getQueue("my-queue").getLocalQueueStats().getMinAge());
        myQueue.add("item-1");
        myQueue.add("item-2");
        sleepAtLeastMillis(50);
        myQueue.take();
        myQueue.take();

        LocalQueueStats stats = member.getQueue("my-queue").getLocalQueueStats();
        assertTrue(stats.getMaxAge() > 0);
        assertTrue(stats.getMinAge() > 0);
        assertTrue(stats.getAverageAge() > 0);

        ClientMessage clientMessage = MCResetQueueAgeStatisticsCodec.encodeRequest("my-queue");
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, "my-queue");
        invocation.invoke().get();
        LocalQueueStats statsAfterReset = member.getQueue("my-queue").getLocalQueueStats();
        assertEquals(0, statsAfterReset.getMaxAge());
        assertEquals(Long.MAX_VALUE, statsAfterReset.getMinAge());
        assertEquals(0, statsAfterReset.getAverageAge());

        myQueue.add("item-3");
        myQueue.add("item-4");
        sleepAtLeastMillis(20);
        myQueue.take();
        sleepAtLeastMillis(100);
        Thread.sleep(100);
        myQueue.take();

        LocalQueueStats populatedStatsAfterReset = member.getQueue("my-queue").getLocalQueueStats();
        assertTrue(statsAfterReset.getMinAge() + " is greater than or equal to 20", statsAfterReset.getMinAge() >= 20);
        assertTrue(statsAfterReset.getMaxAge() > 100);
        assertEquals((populatedStatsAfterReset.getMinAge() + populatedStatsAfterReset.getMaxAge()) / 2,
                populatedStatsAfterReset.getAverageAge()
        );
    }

    @After
    public void shutdown() {
        hazelcastFactory.terminateAll();
    }
}
