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

package com.hazelcast.map;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class SizeEstimatorTest extends HazelcastTestSupport {

    @Test
    public void testIssue833() throws InterruptedException {
        final String MAP_NAME =  "testIssue833";
        Config config = new Config();
        config.getMapConfig( MAP_NAME ).setStatisticsEnabled( true );

        /*NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);*/
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        HazelcastInstance h = factory.newHazelcastInstance( config );

        IMap<String, String> map = h.getMap( MAP_NAME );
        map.put("key", "value");
        map.put("key1", "value1");
        map.put("key2", "value2");

        final long heapCost = map.getLocalMapStats().getHeapCost();
        System.err.println(heapCost);
        final long backupHeapCost = map.getLocalMapStats().getBackupHeapCost();

        assertTrue(heapCost == 0);
        assertTrue( backupHeapCost == 0 );

        h.getLifecycleService().shutdown();
    }

}
