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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This is a 'lite' test that is designed to run with PR builder. It cycles through all the user facing operations exposed by
 * {@link CPMap}. See {@link CPMapProxyTest} and {@link com.hazelcast.cp.internal.datastructures.cpmap.CPMapProxyTest} for the
 * nightly (slower) variants.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CPMapProxyLiteTest extends HazelcastRaftTestSupport {
    private static final String KEY = "k";
    private static final String VALUE = "v";
    private static final String MAP = "map";
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] instances;
    private HazelcastInstance client;

    @Before
    public void before() {
        int nodeCount = 3;
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(nodeCount);
        config.setCPSubsystemConfig(cpSubsystemConfig);
        instances = factory.newInstances(config, nodeCount);
        client = factory.newHazelcastClient();
    }

    private CPMap<String, String> getMap() {
        return client.getCPSubsystem().getMap(MAP);
    }

    @Test
    public void testAllOps() {
        // get
        CPMap<String, String> map = getMap();
        String result = map.get(KEY);
        assertNull(result);

        // put
        assertNull(map.put(KEY, VALUE));
        result = map.get(KEY);
        assertNotNull(result);
        assertEquals(VALUE, result);

        // remove
        assertEquals(VALUE, map.remove(KEY));
        assertNull(map.remove(KEY));

        // putIfAbsent
        assertNull(map.putIfAbsent(KEY, VALUE));
        assertEquals(VALUE, map.putIfAbsent(KEY, "updatedValue"));
        assertEquals(VALUE, map.remove(KEY));

        // set
        map.set(KEY, VALUE);
        result = map.get(KEY);
        assertNotNull(result);
        assertEquals(VALUE, result);

        // delete
        map.delete(KEY);
        result = map.get(KEY);
        assertNull(result);

        // cas
        map.set(KEY, VALUE);
        assertTrue(map.compareAndSet(KEY, VALUE, "v2"));
        result = map.get(KEY);
        assertNotNull(result);
        assertEquals("v2", result);
    }

    @After
    public void after() {
        client.shutdown();
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }
}
