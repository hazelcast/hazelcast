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

package com.hazelcast.client.cp.internal.datastructures.cpmap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.datastructures.cpmap.CPMapProxyCPMapStoreExceptionTest.casExceedMapCapacity;
import static com.hazelcast.cp.internal.datastructures.cpmap.CPMapProxyCPMapStoreExceptionTest.keyValueExceedMapCapacity;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CPMapProxyCPMapStoreExceptionTest extends HazelcastRaftTestSupport {
    private static final String MAP = "map";
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] instances;
    private HazelcastInstance client;
    private CPMap<String, String> map;

    @Before
    public void before() {
        int nodeCount = 3;
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(nodeCount);
        cpSubsystemConfig.addCPMapConfig(new CPMapConfig(MAP, 1));
        config.setCPSubsystemConfig(cpSubsystemConfig);
        instances = factory.newInstances(config, nodeCount);
        client = factory.newHazelcastClient();
        map = client.getCPSubsystem().getMap(MAP);
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

    @After
    public void after() {
        client.shutdown();
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }
}
