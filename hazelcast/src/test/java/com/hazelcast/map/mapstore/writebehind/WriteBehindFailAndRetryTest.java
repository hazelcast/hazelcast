/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindFailAndRetryTest extends HazelcastTestSupport {

    @Test
    public void testStoreOperationDone_afterTemporaryMapStoreFailure() throws Exception {
        final SelfHealingMapStore mapStore = new SelfHealingMapStore<Integer, Integer>();
        final IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .build();

        map.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, mapStore.size());
            }
        });
    }

    @Test
    public void testStoreOperationDone_afterTemporaryMapStoreFailure_whenNonWriteCoalescingModeOn() throws Exception {
        final SelfHealingMapStore mapStore = new SelfHealingMapStore<Integer, Integer>();
        final IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withWriteCoalescing(false)
                .withPartitionCount(1)
                .build();

        map.put(1, 2);
        map.put(1, 3);
        map.put(1, 4);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, mapStore.size());
            }
        });
    }


    static class SelfHealingMapStore<K, V> extends MapStoreAdapter<K, V> {

        private final ConcurrentMap<K, V> store = new ConcurrentHashMap<K, V>();

        private final TemporarySuccessProducer temporarySuccessProducer
                = new TemporarySuccessProducer(10);

        @Override
        public void store(K key, V value) {
            temporarySuccessProducer.successOrException();

            store.put(key, value);
        }

        public int size() {
            return store.size();
        }
    }

    /**
     * Produces periodic success, otherwise there will be many exceptions.
     */
    private static class TemporarySuccessProducer {

        private final long successGenerationPeriodInMillis;

        private volatile long startMillis;

        public TemporarySuccessProducer(long secondsPeriod) {
            this.successGenerationPeriodInMillis = TimeUnit.SECONDS.toMillis(secondsPeriod);
            this.startMillis = Clock.currentTimeMillis();
        }

        public void successOrException() {
            final long now = Clock.currentTimeMillis();
            final long elapsedTime = now - startMillis;

            if (elapsedTime > successGenerationPeriodInMillis) {
                startMillis = Clock.currentTimeMillis();
                return;
            }

            throw new TemporaryMapStoreException();
        }
    }

    private static class TemporaryMapStoreException extends RuntimeException {

        public TemporaryMapStoreException() {
            super("Test exception");
        }
    }


}
