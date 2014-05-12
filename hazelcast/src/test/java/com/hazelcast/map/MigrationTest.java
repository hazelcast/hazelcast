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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MigrationTest extends HazelcastTestSupport {

    @Test
    public void testMapMigration() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        int size = 1000;

        Map map = instance1.getMap("testMapMigration");
        for (int i = 0; i < size; i++) {
            map.put(i,i);
        }

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        Thread.sleep(1000);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        Thread.sleep(1000);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

    }


    @Test
    public void testMigration_failure_when_statistics_disabled() {
        Config config = new Config().addMapConfig(new MapConfig("myMap").setStatisticsEnabled(false));
        int noOfRecords = 100;

        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, Integer> myMap = instance1.getMap("myMap");
        for (int i = 0; i < noOfRecords; i++) {
            myMap.put(i, i);
        }
        instance2.shutdown();
        instance3.shutdown();

        assertEquals("Some records have been lost.", noOfRecords, myMap.values().size());
    }




}
