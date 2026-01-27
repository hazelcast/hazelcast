/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.AbstractClockTest;
import com.hazelcast.internal.util.ClockProperties;
import com.hazelcast.internal.util.TestStoppedClock;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MapStoreWriteBehindClockTest extends AbstractClockTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    HazelcastInstance client;

    public static class StaticStoreCounterMapStore extends MapStoreAdapter<Integer, Integer> {

        private static final AtomicInteger storeCounter = new AtomicInteger(0);

        @Override
        public void store(final Integer key, final Integer value) {
            storeCounter.incrementAndGet();
        }

        public int getStoreCounter() {
            return storeCounter.get();
        }
    }

    @After
    public void after() {
        shutdownIsolatedNode();
        resetClock();
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testNoFlush_than_noStore() {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_IMPL, TestStoppedClock.class.getName());
        var mapName = "testMap";

        Config config = smallInstanceConfig();
        config.getMapConfig(mapName)
            .getMapStoreConfig()
            .setEnabled(true)
            .setClassName(StaticStoreCounterMapStore.class.getName())
            .setInitialLoadMode(EAGER)
            .setWriteDelaySeconds(Integer.MAX_VALUE);
        startIsolatedNode(config);

        var map = isolatedNode.getMap(mapName);
        map.put(0, 0);
        assertEquals(1, map.size());

        assertTrueAllTheTime(
            () -> {
                int storeCounter = invokeIsolatedInstanceMethod(StaticStoreCounterMapStore.class.getName(), "getStoreCounter");
                assertThat(storeCounter).isEqualTo(0);
            },
            TIMEOUT.getSeconds()
        );
    }
}
