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

package com.hazelcast.client.txn;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnDisconnectionTest {

    private static final String BOUNDED_QUEUE_PREFIX = "bounded-queue-*";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private OperationParkerImpl waitNotifyService;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        Config config = new Config();
        config.getQueueConfig(BOUNDED_QUEUE_PREFIX).setMaxSize(1);
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        NodeEngineImpl nodeEngine = getNode(instance).nodeEngine;
        waitNotifyService = (OperationParkerImpl) nodeEngine.getOperationParker();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void testQueueTake() {
        testQueue(new Callable() {
            @Override
            public Object call() throws InterruptedException {
                TransactionContext context = client.newTransactionContext();
                context.beginTransaction();
                TransactionalQueue<Object> queue = context.getQueue(randomString());
                return queue.take();
            }
        });
    }

    @Test
    public void testQueuePoll() {
        testQueue(new Callable() {
            @Override
            public Object call() throws InterruptedException {
                TransactionContext context = client.newTransactionContext();
                context.beginTransaction();
                TransactionalQueue<Object> queue = context.getQueue(randomString());
                return queue.poll(20, SECONDS);
            }
        });
    }

    @Test
    public void testQueueOffer() {
        testQueue(new Callable() {
            @Override
            public Object call() throws InterruptedException {
                String name = BOUNDED_QUEUE_PREFIX + randomString();
                client.getQueue(name).offer(randomString());

                TransactionContext context = client.newTransactionContext();
                context.beginTransaction();
                TransactionalQueue<Object> queue = context.getQueue(name);
                return queue.offer(randomString(), 20, SECONDS);
            }
        });
    }

    private void testQueue(Callable task) {
        spawn(task);
        assertValidWaitingOperationCount(1);
        client.shutdown();
        assertValidWaitingOperationCount(0);
    }

    private void assertValidWaitingOperationCount(final int count) {

        // Note: The wait duration here should be more than endpoint remove delay time
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(count, waitNotifyService.getTotalValidWaitingOperationCount());
            }
        });
    }
}
