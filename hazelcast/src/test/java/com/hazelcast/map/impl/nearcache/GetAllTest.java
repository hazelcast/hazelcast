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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetAllTest extends HazelcastTestSupport {

    @Test
    public void ensure_supplied_number_of_keys_are_in_near_cache() throws Exception {
        final int entryCount = 100000;
        final String mapName = "test";

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(true);

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap(mapName);

        for (int i = 0; i < entryCount; i++) {
            map.put(i, i);
        }

        HashSet keys = new HashSet();
        for (int i = 0; i < entryCount; i++) {
            keys.add(i);
        }

        map.getAll(keys);

        assertEquals(entryCount, ((NearCachedMapProxyImpl) map).getNearCache().size());
    }
}
