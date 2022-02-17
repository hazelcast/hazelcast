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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.UserCodeDeploymentConfig.ClassCacheMode.ETERNAL;
import static com.hazelcast.config.UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

// Test resolution of user customization class names in dynamic data structure config via user code deployment
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigAdvancedTest {

    private static final int CLUSTER_SIZE = 3;
    private static final String MAP_NAME = "map-with-maploader";

    private TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_userCustomizations_withUserCodeDeployment() {
        factory = new TestHazelcastInstanceFactory();
        // first member is aware of MapLoader class
        HazelcastInstance member1 = factory.newHazelcastInstance(newConfigWithUserCodeDeployment());
        member1.getConfig().addMapConfig(mapConfigWithMapLoader());

        IMap<Integer, Integer> intMap = member1.getMap(MAP_NAME);
        assertEquals(1, (long) intMap.get(1));

        // start another member which is not aware of map loader
        Config config = newConfigWithUserCodeDeployment();
        FilteringClassLoader cl = new FilteringClassLoader(singletonList("classloading"), null);
        config.setClassLoader(cl);
        factory.newHazelcastInstance(config);

        for (int i = 0; i < 1000; i++) {
            assertEquals(i, (long) intMap.get(i));
        }
    }

    private MapConfig mapConfigWithMapLoader() {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.getMapStoreConfig()
                .setEnabled(true)
                .setClassName("classloading.domain.IntMapLoader");
        return mapConfig;
    }

    private Config newConfigWithUserCodeDeployment() {
        Config config = new Config();
        config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(ETERNAL)
                .setProviderMode(LOCAL_AND_CACHED_CLASSES)
                .setWhitelistedPrefixes("classloading");
        return config;
    }

}
