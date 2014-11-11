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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindFailAndRetryTest extends HazelcastTestSupport {

    @Test
    public void testStoreOperationDone_afterTemporaryMapStoreFailure() throws Exception {
        final SelfHealingMapStore mapStore = new SelfHealingMapStore<Integer, Integer>();
        final IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
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


    static class SelfHealingMapStore<K, V> extends MapStoreAdapter<K, V> {

        private static final long DIFF = TimeUnit.SECONDS.toMillis(3);

        private ConcurrentMap<K, V> store = new ConcurrentHashMap<K, V>();

        private ConcurrentMap<String, Long> timeHolder = new ConcurrentHashMap<String, Long>();

        private static final String TIME_KEY = "time";

        @Override
        public void store(K key, V value) {
            final Object time = timeHolder.get(TIME_KEY);
            if (time == null) {
                timeHolder.put(TIME_KEY, System.currentTimeMillis());
            } else {
                final long now = System.currentTimeMillis();
                final long diff = now - (Long) time;
                if (diff > DIFF) {
                    store.put(key, value);
                    return;
                }
            }
            throw new RuntimeException("Temporary map store failure");
        }

        public int size() {
            return store.size();
        }
    }
}
