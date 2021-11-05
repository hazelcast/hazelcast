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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MapInfoCollectorTest extends HazelcastTestSupport {

    @Test
    public void mapInfoCollector_does_not_create_new_map_config() {
        String mapName = "hello";

        MapInfoCollector mapInfoCollector = new MapInfoCollector();

        HazelcastInstance hzInstance = createHazelcastInstance(new Config());
        hzInstance.getMap(mapName);

        mapInfoCollector.forEachMetric(Accessors.getNode(hzInstance), (phoneHomeMetrics, s) -> {
        });

        Set<String> mapNames = hzInstance.getConfig().getMapConfigs().keySet();

        assertEquals(1, mapNames.size());
        assertTrue(mapNames.contains("default"));
        assertFalse(mapNames.contains(mapName));

    }
}
