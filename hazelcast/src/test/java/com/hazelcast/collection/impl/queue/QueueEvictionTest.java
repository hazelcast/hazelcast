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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueEvictionTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testQueueEviction_whenTtlIsSet_thenTakeThrowsException() throws Exception {
        String queueName = randomString();

        Config config = smallInstanceConfig();
        config.getQueueConfig(queueName)
              .setPriorityComparatorClassName(comparatorClassName)
              .setEmptyQueueTtl(2);
        HazelcastInstance hz = createHazelcastInstance(config);
        IQueue<VersionedObject<String>> queue = hz.getQueue(queueName);

        try {
            assertTrue(queue.offer(new VersionedObject<>("item")));
            assertEquals(new VersionedObject<>("item"), queue.poll());

            queue.take();
            fail();
        } catch (DistributedObjectDestroyedException expected) {
            ignore(expected);
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testQueueEviction_whenTtlIsZero_thenListenersAreNeverthelessExecuted() throws Exception {
        String queueName = randomString();

        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        config.getQueueConfig(queueName).setEmptyQueueTtl(0);
        HazelcastInstance hz = createHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        IQueue<VersionedObject<String>> queue = hz.getQueue(queueName);
        assertTrue(queue.offer(new VersionedObject<>("item")));
        assertEquals(new VersionedObject<>("item"), queue.poll());

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
