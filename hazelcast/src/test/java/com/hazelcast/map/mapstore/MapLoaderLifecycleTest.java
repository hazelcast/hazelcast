/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Properties;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapLoaderLifecycleTest extends HazelcastTestSupport {

    private MapLoaderLifecycleSupport loader = mockMapLoaderWithLifecycle();
    private Config config = new Config();

    @Before
    public void configure() {
        config.getMapConfig("map").setMapStoreConfig(new MapStoreConfig().setImplementation(loader));
    }

    @Test
    public void testInitCalled_whenMapCreated() throws Exception {

        HazelcastInstance hz = createHazelcastInstance(config);

        hz.getMap("map");

        verify(loader).init(eq(hz), eq(new Properties()), eq("map"));
    }

    @Test
    public void testDestroyCalled_whenNodeShutdown() throws Exception {

        HazelcastInstance hz = createHazelcastInstance(config);

        hz.getMap("map");
        hz.shutdown();

        verify(loader).destroy();
    }

    private static MapLoaderLifecycleSupport mockMapLoaderWithLifecycle() {
        return mock(MapLoaderLifecycleSupport.class, withSettings().extraInterfaces(MapLoader.class));
    }
}
