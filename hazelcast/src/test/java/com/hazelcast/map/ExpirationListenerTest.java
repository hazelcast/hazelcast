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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpirationListenerTest extends HazelcastTestSupport {

    private static final int instanceCount = 3;

    private HazelcastInstance[] instances;
    private IMap<Integer, Integer> map;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        instances = factory.newInstances(config);
        HazelcastInstance randomNode = instances[RandomPicker.getInt(instanceCount)];
        map = randomNode.getMap(randomMapName());
    }

    @Test
    public void test_expiration_listener_is_notified_after_expiration_of_entries() {
        int numberOfPutOperations = 1000;
        CountDownLatch expirationEventArrivalCount = new CountDownLatch(numberOfPutOperations);

        map.addEntryListener(new ExpirationListener(expirationEventArrivalCount), true);

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.put(i, i, 100, TimeUnit.MILLISECONDS);
        }

        // wait expiration of entries
        sleepAtLeastMillis(200);

        // trigger immediate fire of expiration events by touching them
        for (int i = 0; i < numberOfPutOperations; i++) {
            map.get(i);
        }

        assertOpenEventually(expirationEventArrivalCount);
    }

    @Test
    public void test_when_ttl_is_modified_expiration_listener_is_notified_after_expiration_of_entries() {
        int numberOfPutOperations = 1000;
        CountDownLatch expirationEventArrivalCount = new CountDownLatch(numberOfPutOperations);

        map.addEntryListener(new ExpirationListener(expirationEventArrivalCount), true);

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.setTtl(i, 100, TimeUnit.MILLISECONDS);
        }

        // wait expiration of entries
        sleepAtLeastMillis(200);

        // trigger immediate fire of expiration events by touching them
        for (int i = 0; i < numberOfPutOperations; i++) {
            map.get(i);
        }

        assertOpenEventually(expirationEventArrivalCount);
    }

    @Test
    public void testExpirationListener_notified_one_time_with_imap_locking() {
        AtomicInteger eventCounter = new AtomicInteger();
        EntryExpiredListener listener = new LockingOverIMapExpirationListener(eventCounter, map);

        testListenerIsNotifiedOneTime(eventCounter, listener);
    }

    @Test
    public void testExpirationListener_notified_one_time_with_cp_subsystem_locking() {
        AtomicInteger eventCounter = new AtomicInteger();
        EntryExpiredListener listener
                = new LockingOverCpSubsystemExpirationListener(eventCounter, instances[0].getCPSubsystem());

        testListenerIsNotifiedOneTime(eventCounter, listener);
    }

    private void testListenerIsNotifiedOneTime(AtomicInteger eventCounter, EntryExpiredListener entryExpiredListener) {
        int numberOfPutOperations = 100;

        map.addEntryListener(entryExpiredListener, true);

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.put(i, i, 100, TimeUnit.MILLISECONDS);
        }

        assertTrueEventually(() -> assertEquals("Map size is not zero", 0, map.size()));
        assertTrueEventually(() -> assertEquals("Received unexpected number of events",
                numberOfPutOperations, eventCounter.get()));
    }


    private static class ExpirationListener implements EntryExpiredListener {

        private final CountDownLatch expirationEventCount;

        ExpirationListener(CountDownLatch expirationEventCount) {
            this.expirationEventCount = expirationEventCount;
        }

        @Override
        public void entryExpired(EntryEvent event) {
            expirationEventCount.countDown();
        }
    }

    private static class LockingOverIMapExpirationListener implements EntryExpiredListener {

        private final IMap map;
        private final AtomicInteger eventCounter;

        LockingOverIMapExpirationListener(AtomicInteger eventCounter, IMap map) {
            this.map = map;
            this.eventCounter = eventCounter;
        }

        @Override
        public void entryExpired(EntryEvent event) {
            eventCounter.incrementAndGet();

            Object key = event.getKey();
            map.lock(key);
            try {
                // mimic a slow execution
                sleepMillis(10);
            } finally {
                map.unlock(key);
            }
        }
    }

    private static class LockingOverCpSubsystemExpirationListener implements EntryExpiredListener {

        private final CPSubsystem cpSubsystem;
        private final AtomicInteger eventCounter;

        LockingOverCpSubsystemExpirationListener(AtomicInteger eventCounter, CPSubsystem cpSubsystem) {
            this.cpSubsystem = cpSubsystem;
            this.eventCounter = eventCounter;
        }

        @Override
        public void entryExpired(EntryEvent event) {
            eventCounter.incrementAndGet();

            Object key = event.getKey();
            FencedLock lock = cpSubsystem.getLock(key.toString());
            lock.lock();
            try {
                // mimic a slow execution
                sleepMillis(100);
            } finally {
                lock.unlock();
            }
        }
    }
}
