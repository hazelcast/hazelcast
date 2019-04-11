/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExpirationManagerTimeoutTest extends HazelcastTestSupport {

    @Test
    public void afterShortExpirationEntryShouldBeAway() throws InterruptedException {
        final String KEY = "key";

        Config hConfig = new Config()
                .addMapConfig(new MapConfig().setName("test")
                        .setMaxSizeConfig(new MaxSizeConfig(200, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE))
                        .setTimeToLiveSeconds(20));
        final HazelcastInstance node = createHazelcastInstance(hConfig);
        try {
            IMap<String, String> map = node.getMap("test");
            // after 1 second entry should be evicted
            map.put(KEY, "value", 1, TimeUnit.SECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);

            Thread.sleep(1200);

            // more than one second after adding it, now it should be away
            map.lock(KEY);
            object = map.get(KEY);
            map.unlock(KEY);
            assertNull(object);
        } finally {
            node.shutdown();
        }
    }

    @Test
    public void afterLongerExpirationEntryShouldBeAway() throws InterruptedException {
        final String KEY = "key";

        Config hConfig = new Config()
                .addMapConfig(new MapConfig().setName("test")
                        .setMaxSizeConfig(new MaxSizeConfig(200, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE))
                        .setTimeToLiveSeconds(20));
        final HazelcastInstance node = createHazelcastInstance(hConfig);
        try {
            IMap<String, String> map = node.getMap("test");
            // after 1 second entry should be evicted
            map.put(KEY, "value", 1, TimeUnit.SECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);
            Thread.sleep(3600);
            // More than one second after adding it, now it should be away
            map.lock(KEY);
            object = map.get(KEY);
            map.unlock(KEY);
            assertNull(object);
        } finally {
            node.shutdown();
        }
    }
}
