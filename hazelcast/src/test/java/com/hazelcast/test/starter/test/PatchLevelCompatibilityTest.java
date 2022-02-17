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

package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@Ignore("https://github.com/hazelcast/hazelcast/issues/15021")
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class PatchLevelCompatibilityTest {

    private HazelcastInstance[] instances;

    @After
    public void tearDown() {
        if (instances != null) {
            for (HazelcastInstance hz: instances) {
                if (hz != null) {
                    hz.shutdown();
                }
            }
        }
    }

    @Test
    public void testAll_V37_Versions() {
        String[] versions = new String[]{"3.7", "3.7.1", "3.7.2", "3.7.3", "3.7.4", "3.7.5", "3.7.6", "3.7.7", "3.7.8"};
        testAllGivenVersions(versions);
    }

    @Test
    public void testAll_V38_Versions() {
        String[] versions = new String[]{"3.8", "3.8.1", "3.8.2", "3.8.3", "3.8.4", "3.8.5", "3.8.6", "3.8.7", "3.8.8", "3.8.9"};
        testAllGivenVersions(versions);
    }

    @Test
    public void testAll_V39_Versions() {
        String[] versions = new String[]{"3.9", "3.9.1", "3.9.2", "3.9.3", "3.9.4"};
        testAllGivenVersions(versions);
    }

    @Test
    public void testAll_V310_Versions() {
        String[] versions = new String[]{"3.10", "3.10.1", "3.10.2", "3.10.3", "3.10.4"};
        testAllGivenVersions(versions);
    }

    @Test
    public void testMap_whenMixed_V37_Cluster() {
        String[] versions = new String[]{"3.7.4", "3.7.5"};
        testAllGivenVersions(versions);

        IMap<Integer, String> map374 = instances[0].getMap("myMap");
        map374.put(42, "GUI = Cheating!");

        IMap<Integer, String> map375 = instances[1].getMap("myMap");
        assertEquals("GUI = Cheating!", map375.get(42));
    }

    private void testAllGivenVersions(String[] versions) {
        Config config = smallInstanceConfig();
        instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.newHazelcastInstance(versions[i], config, false);
        }
        assertClusterSizeEventually(versions.length, instances[0]);
    }
}
