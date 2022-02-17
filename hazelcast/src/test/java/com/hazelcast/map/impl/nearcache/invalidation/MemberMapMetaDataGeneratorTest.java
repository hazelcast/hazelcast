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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapMetaDataGeneratorTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "MemberMapMetaDataGeneratorTest";

    @Test
    public void destroying_map_removes_related_metadata_when_near_cache_exists() {
        MapConfig mapConfig = getMapConfig(MAP_NAME);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance member = createHazelcastInstance(config);

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(1, 1);

        final MetaDataGenerator metaDataGenerator = getMetaDataGenerator(member);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
            }
        });

        map.destroy();

        assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
    }

    @Test
    public void destroying_map_removes_related_metadata_when_near_cache_not_exists() {
        Config config = getConfig();
        HazelcastInstance member = createHazelcastInstance(config);

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(1, 1);

        final MetaDataGenerator metaDataGenerator = getMetaDataGenerator(member);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
            }
        });

        map.destroy();

        assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
    }

    protected Config getConfig() {
        return getBaseConfig();
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName);
    }

    private static MetaDataGenerator getMetaDataGenerator(HazelcastInstance member) {
        MapService mapService = getNodeEngineImpl(member).getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }
}
