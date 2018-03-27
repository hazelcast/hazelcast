/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class PluggableMemoryInfoAccessorTest extends HazelcastTestSupport {

    private static final String HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL = "hazelcast.memory.info.accessor.impl";

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL,
            ZeroMemoryInfoAccessor.class.getCanonicalName());


    /**
     * Used {@link ZeroMemoryInfoAccessor} to evict every put, map should not contain any entry after this test run.
     */
    @Test
    public void testPluggedMemoryInfoAccessorUsed() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(FREE_HEAP_PERCENTAGE);
        maxSizeConfig.setSize(50);

        Config config = getConfig();
        config.getMapConfig("test").setEvictionPolicy(LFU).setMaxSizeConfig(maxSizeConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("test");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        assertEquals(0, map.size());
    }


}
