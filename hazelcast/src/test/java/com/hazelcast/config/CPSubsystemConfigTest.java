/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPSubsystemConfigTest extends HazelcastTestSupport {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(CPSubsystemConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    // TODO: if there's no config found for [m] then we need to use defaults
    // from what I see, the query must always be: map-name@group-name

    @Test
    public void testCPMapConfig() {
        String mapName = "map1";
        CPMapConfig config = new CPMapConfig(mapName);
        assertEquals(CPMapConfig.DEFAULT_MAX_SIZE_MB, config.getMaxSizeMb());
        assertEquals(mapName, config.getName());
    }

    @Test
    public void testCPMapConfig_Add() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        CPMapConfig map1 = new CPMapConfig("map1");
        CPMapConfig map2 = new CPMapConfig("map2");
        config.addCPMapConfig(map1).addCPMapConfig(map2);
        assertEquals(map1, config.findCPMapConfig(map1.getName()));
        assertEquals(map2, config.findCPMapConfig(map2.getName()));
    }

    @Test
    public void testCPMapConfig_Get() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        CPMapConfig map1 = new CPMapConfig("map1");
        CPMapConfig map2 = new CPMapConfig("map2");
        config.addCPMapConfig(map1).addCPMapConfig(map2);
        Map<String, CPMapConfig> expected = Map.of(map1.getName(), map1, map2.getName(), map2);
        assertEquals(expected, config.getCpMapConfigs());
    }

    @Test
    public void testCPMapConfig_Set() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        CPMapConfig map1 = new CPMapConfig("map1");
        CPMapConfig map2 = new CPMapConfig("map2");
        Map<String, CPMapConfig> mapConfigs = Map.of(map1.getName(), map1, map2.getName(), map2);
        config.setCPMapConfigs(mapConfigs);
        assertEquals(mapConfigs, config.getCpMapConfigs());
    }
}
