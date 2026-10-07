/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CPMapRegistryTest {
    // The way that findXxx methods work is rudimentary: it always strips off the @group suffix, so it implies the map names (and
    // other CP object names) themselves are unique across all groups. Seems wrong to me. The 'getMapMaxSize' family of tests
    // conform to this basic semantics.
    private static final Map<String, Integer> MAPS =
            Map.of("map1", 20, "map2", 10, "map3", 5, "map4", 1);
    private CPSubsystemConfig cpSubsystemConfig;

    @Before
    public void before() {
        cpSubsystemConfig = new CPSubsystemConfig();
        MAPS.forEach((key, value) -> cpSubsystemConfig.addCPMapConfig(createMapConfig(key, value)));
    }

    @Test
    public void getMapMaxSizeMb_NonExistentConfigUsesDefault() {
        CPMapConfig mapConfig = cpSubsystemConfig.findCPMapConfig("map5");
        int actual = CPMapRegistry.getMapMaxSizeMb(mapConfig);
        assertEquals(CPMapRegistry.DEFAULT_MAP_MAX_SIZE_MB, actual);
    }

    @Test
    public void getMapMaxSizeMb_WhenDefined() {
        for (Map.Entry<String, Integer> e : MAPS.entrySet()) {
            CPMapConfig mapConfig = cpSubsystemConfig.findCPMapConfig(e.getKey());
            Integer actual = CPMapRegistry.getMapMaxSizeMb(mapConfig);
            assertEquals(e.getValue(), actual);
        }
    }

    private static CPMapConfig createMapConfig(String name, int maxSizeMb) {
        CPMapConfig config = new CPMapConfig();
        config.setMaxSizeMb(maxSizeMb);
        config.setName(name);
        return config;
    }
}
