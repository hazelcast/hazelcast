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

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CPMapProxyCPMapStoreExceptionTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "mymap";
    private static final int KV_WRITES_UNTIL_CAPACITY_EXCEEDED = 244;
    HazelcastInstance[] instances;
    CPMap<String, String> map;

    @Before
    public void before() {
        int nodeCount = 3;
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(nodeCount);
        cpSubsystemConfig.addCPMapConfig(new CPMapConfig(MAP_NAME, 1));
        config.setCPSubsystemConfig(cpSubsystemConfig);
        instances = createHazelcastInstances(config, nodeCount);
        map = instances[0].getCPSubsystem().getMap(MAP_NAME);
    }

    @Test
    public void testPut() {
        keyValueExceedMapCapacity((key, value) -> map.put(key, value));
    }

    @Test
    public void testSet() {
        keyValueExceedMapCapacity((key, value) -> map.set(key, value));
    }

    @Test
    public void testCas() {
        casExceedMapCapacity(map);
    }

    /**
     * Exceeds a map capacity when the capacity of the map is 1MB.
     */
    public static void casExceedMapCapacity(CPMap<String, String> map) {
        String value = generateRandomString(4096);
        for (int i = 0; i < (KV_WRITES_UNTIL_CAPACITY_EXCEEDED - 1); i++) {
            map.set(String.valueOf(i), value);
        }
        Throwable t = assertThrows(IllegalStateException.class,
                () -> map.compareAndSet(String.valueOf(242), value, generateRandomString(4096)));
        assertEquals("Write not permitted as it would exceed the user defined capacity limit of 1MB", t.getMessage());
    }

    /**
     * Exceeds a map capacity when the capacity of the map is 1MB.
     */
    public static void keyValueExceedMapCapacity(BiConsumer<String, String> consumer) {
        String value = generateRandomString(4096);
        Throwable t = assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < KV_WRITES_UNTIL_CAPACITY_EXCEEDED; i++) {
                consumer.accept(String.valueOf(i), value);
            }
        });
        assertEquals("Write not permitted as it would exceed the user defined capacity limit of 1MB", t.getMessage());
    }
}
