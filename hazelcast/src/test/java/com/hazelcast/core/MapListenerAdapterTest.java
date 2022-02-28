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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapListenerAdapterTest extends HazelcastTestSupport {

    @Test
    public void testMapListenerAdapter_whenEntryExpired() {
        String mapName = randomMapName();
        Config cfg = new Config();

        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = instanceFactory.newHazelcastInstance(cfg);

        IMap map = instance.getMap(mapName);

        final CountDownLatch expirationLatch = new CountDownLatch(1);
        map.addEntryListener(new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                expirationLatch.countDown();
            }
        }, false);

        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        sleepSeconds(1);

        // trigger immediate expiration.
        map.get(1);

        assertOpenEventually(expirationLatch);
    }

}
