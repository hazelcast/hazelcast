/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EventualConsistentNearCacheTest extends HazelcastTestSupport {

    TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Test
    public void name() throws Exception {
        Config config = getConfig();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "false");
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), Integer.toString(10));
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(1));
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node1 = factory.newHazelcastInstance(config);
//        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        IMap map1 = node1.getMap("test");
//        IMap map2 = node1.getMap("test2");
//        IMap map3 = node1.getMap("test3");

        for (int i = 0; i < 100; i++) {
            map1.put(i, i);
//            map2.put(i, i);
//            map3.put(i, i);
        }
sleepSeconds(2);
        for (int i = 0; i < 100; i++) {
            map1.put(i, i);
//            map2.put(i, i);
//            map3.put(i, i);
        }

        sleepSeconds(1);
        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        return new Config();
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig();
    }

}
