/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheConfigAccessorTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(NearCacheConfigAccessor.class);
    }

    @Test
    public void testNearCacheConfigEqual_BeforeAndAfterDefaultSizeIsAccesed() {
        NearCacheConfig config1 = new NearCacheConfig();
        NearCacheConfig config2 = new NearCacheConfig();
        int size = NearCacheConfigAccessor.getDefaultMaxSizeForOnHeapMaps(config1);
        assertEquals(MapConfig.DEFAULT_MAX_SIZE, size);
        assertEquals(config1, config2);
    }

    @Test
    public void testEvictionConfigCopy_whenDefaultSizeIsInitialized() {
        NearCacheConfig config1 = new NearCacheConfig();
        NearCacheConfigAccessor.getDefaultMaxSizeForOnHeapMaps(config1);

        EvictionConfig evictionConfig = config1.getEvictionConfig();
        EvictionConfig evictionConfig2 = new EvictionConfig(evictionConfig);

        assertEquals(evictionConfig.getSize(), evictionConfig2.getSize());
    }
}
