/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PatchLevelCompatibilityTest {

    @Test
    public void testAll_V37_Versions() {
        String[] versions = new String[]{"3.7", "3.7.1", "3.7.2", "3.7.3", "3.7.4", "3.7.5", "3.7.6", "3.7.7"};
        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.newHazelcastInstance(versions[i]);
        }
        assertClusterSizeEventually(versions.length, instances[0]);
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Test
    public void testAll_V38_Versions() {
        String[] versions = new String[]{"3.8", "3.8.1", "3.8.2"};
        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.newHazelcastInstance(versions[i]);
        }
        assertClusterSizeEventually(versions.length, instances[0]);
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Test
    public void testMap_whenMixed_V37_Cluster() {
        HazelcastInstance hz374 = HazelcastStarter.newHazelcastInstance("3.7.4");
        HazelcastInstance hz375 = HazelcastStarter.newHazelcastInstance("3.7.5");

        IMap<Integer, String> map374 = hz374.getMap("myMap");
        map374.put(42, "GUI = Cheating!");

        IMap<Integer, String> myMap = hz375.getMap("myMap");
        String ancientWisdom = myMap.get(42);

        assertEquals("GUI = Cheating!", ancientWisdom);
        hz374.shutdown();
        hz375.shutdown();
    }
}
