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
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueStatisticsTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    private HazelcastInstance instance;

    @Test
    public void testItemCount() {
        IQueue<VersionedObject<String>> queue = newQueue();
        int items = 20;
        for (int i = 0; i < items; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(20, stats.getOwnedItemCount());
        assertEquals(0, stats.getBackupItemCount());
    }

    @Test
    public void testOfferOperationCount() throws Exception {
        IQueue<VersionedObject<String>> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        for (int i = 0; i < 10; i++) {
            queue.add(new VersionedObject<>("item" + i, i));
        }
        for (int i = 0; i < 10; i++) {
            queue.put(new VersionedObject<>("item" + i, i));
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertTrueEventually(() -> assertEquals(30, stats.getOfferOperationCount()));
    }

    @Test
    public void testRejectedOfferOperationCount() {
        IQueue<VersionedObject<String>> queue = newQueue(30);
        for (int i = 0; i < 30; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertTrueEventually(() -> assertEquals(10, stats.getRejectedOfferOperationCount()));
    }

    @Test
    public void testPollOperationCount() throws Exception {
        IQueue<VersionedObject<String>> queue = newQueue();
        for (int i = 0; i < 30; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        for (int i = 0; i < 10; i++) {
            queue.remove();
        }
        for (int i = 0; i < 10; i++) {
            queue.take();
        }
        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertTrueEventually(() -> assertEquals(30, stats.getPollOperationCount()));
    }

    @Test
    public void testEmptyPollOperationCount() {
        IQueue<VersionedObject<String>> queue = newQueue();

        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertTrueEventually(() -> assertEquals(10, stats.getEmptyPollOperationCount()));
    }

    @Test
    public void testOtherOperationCount() {
        IQueue<VersionedObject<String>> queue = newQueue();
        for (int i = 0; i < 30; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        ArrayList<VersionedObject<String>> list = new ArrayList<>();
        queue.drainTo(list);
        queue.addAll(list);
        queue.removeAll(list);

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertTrueEventually(() -> assertEquals(3, stats.getOtherOperationsCount()));
    }

    @Test
    public void testAge()
            throws InterruptedException {
        IQueue<VersionedObject<String>> queue = newQueue();
        queue.offer(new VersionedObject<>("maxAgeItem", 0));
        queue.offer(new VersionedObject<>("minAgeItem", 1));
        sleepAtLeastMillis(100);
        queue.poll();
        queue.poll();

        QueueService queueService = getNode(instance).nodeEngine.getService(QueueService.SERVICE_NAME);
        LocalQueueStats stats = queueService.getStats().get(queue.getName());

        long maxAge = stats.getMaxAge();
        long minAge = stats.getMinAge();
        long expectedAverageAge = (maxAge + minAge) / 2;
        long avgAge = stats.getAverageAge();
        assertEquals(expectedAverageAge, avgAge);
    }

    @Test
    public void testEventOperationCount() {
        IQueue<VersionedObject<String>> queue = newQueue();
        TestListener listener = new TestListener(30);
        queue.addItemListener(listener, true);

        for (int i = 0; i < 30; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        for (int i = 0; i < 30; i++) {
            queue.poll();
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertOpenEventually(listener.addedLatch);
        assertOpenEventually(listener.removedLatch);
        assertTrueEventually(() -> assertEquals(60, stats.getEventOperationCount()));
    }

    private IQueue<VersionedObject<String>> newQueue() {
        return newQueue(QueueConfig.DEFAULT_MAX_SIZE);
    }

    private IQueue<VersionedObject<String>> newQueue(int maxSize) {
        String name = randomString();
        Config config = smallInstanceConfig();
        config.getQueueConfig(name)
              .setPriorityComparatorClassName(comparatorClassName)
              .setMaxSize(maxSize);
        instance = createHazelcastInstance(config);
        return instance.getQueue(name);
    }

    private static class TestListener implements ItemListener<VersionedObject<String>> {
        final CountDownLatch addedLatch;
        final CountDownLatch removedLatch;

        TestListener(int latchCount) {
            addedLatch = new CountDownLatch(latchCount);
            removedLatch = new CountDownLatch(latchCount);
        }

        public void itemAdded(ItemEvent<VersionedObject<String>> item) {
            addedLatch.countDown();
        }

        public void itemRemoved(ItemEvent<VersionedObject<String>> item) {
            removedLatch.countDown();
        }
    }
}
