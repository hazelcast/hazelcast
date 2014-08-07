package com.hazelcast.core;
/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EntryAdapterTest extends HazelcastTestSupport {
    @Test
    public void testEntryAdapterMapEvicted() {
        String mapName = randomMapName();
        Config cfg = new Config();

        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = instanceFactory.newHazelcastInstance(cfg);

        IMap map = instance.getMap(mapName);
        map.put(1, 1);
        map.put(2, 2);

        final CountDownLatch evictionLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter() {
            public void mapEvicted(final MapEvent event) {
                evictionLatch.countDown();
            }
        }, false);

        map.evictAll();

        assertOpenEventually(evictionLatch);
    }

    @Test
    public void testEntryAdapterMapCleared() {
        String mapName = randomMapName();
        Config cfg = new Config();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = instanceFactory.newHazelcastInstance(cfg);

        IMap map = instance.getMap(mapName);
        map.put(1, 1);
        map.put(2, 2);

        final CountDownLatch clearLatch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter() {
            @Override
            public void mapCleared(MapEvent event) {
                clearLatch.countDown();
            }
        }, false);

        map.clear();

        assertOpenEventually(clearLatch);
    }

}
