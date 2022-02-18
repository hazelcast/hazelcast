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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpirationListenerTest extends HazelcastTestSupport {

    private static final int instanceCount = 3;

    private HazelcastInstance[] instances;
    private IMap<Integer, Integer> map;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        Config config = new Config();
        instances = factory.newInstances(config);
        HazelcastInstance randomNode = instances[RandomPicker.getInt(instanceCount)];
        map = randomNode.getMap(randomMapName());
    }

    @Test
    public void testExpirationListener_notified_afterExpirationOfEntries() throws Exception {
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
    public void test_whenTTLisModified_ExpirationListenernotified_afterExpirationOfEntries() throws Exception {
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
}
