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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
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




}
