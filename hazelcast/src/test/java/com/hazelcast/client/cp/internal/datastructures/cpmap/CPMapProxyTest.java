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
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CPMapProxyTest extends HazelcastRaftTestSupport {

    @ClassRule
    public static final ChangeLoggingRule CHANGE_LOGGING_RULE
            = new ChangeLoggingRule("log4j2-cp-purge-debug.xml");

    private static final String KEY = "k";
    private static final String VALUE = "v";
    private static final String MAP = "map";
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] instances;
    private HazelcastInstance client;

    @Before
    public void before() {
        int nodeCount = 3;
        Config config = getConfig(nodeCount);
        instances = factory.newInstances(config, nodeCount);
        client = factory.newHazelcastClient();
    }

    private @NonNull Config getConfig(int nodeCount) {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(nodeCount);
        return config.setCPSubsystemConfig(cpSubsystemConfig);
    }

    private CPMap<String, String> getMap() {
        return client.getCPSubsystem().getMap(MAP);
    }

    @Test
    public void testGet() {
        CPMap<String, String> map = getMap();
        String result = map.get(KEY);
        assertNull(result);
    }

    @Test
    public void testPut() {
        CPMap<String, String> map = getMap();
        assertNull(map.put(KEY, VALUE));
        String result = map.get(KEY);
        assertNotNull(result);
        assertEquals(VALUE, result);
    }

    @Test
    public void testSet() {
        CPMap<String, String> map = getMap();
        map.set(KEY, VALUE);
        String result = map.get(KEY);
        assertNotNull(result);
        assertEquals(VALUE, result);
    }

    @Test
    public void testPutIfAbsent() {
        CPMap<String, String> map = getMap();
        assertNull(map.putIfAbsent(KEY, VALUE));
        String v2 = "v2";
        assertEquals(VALUE, map.putIfAbsent(KEY, v2));
        assertEquals(VALUE, map.get(KEY));
    }

    @Test
    public void testRemove() {
        CPMap<String, String> map = getMap();
        assertNull(map.remove(KEY));
        map.set(KEY, VALUE);
        String result = map.remove(KEY);
        assertNotNull(result);
        assertEquals(VALUE, result);
        assertNull(map.remove(KEY));
    }

    @Test
    public void testDelete() {
        CPMap<String, String> map = getMap();
        map.set(KEY, VALUE);
        map.delete(KEY);
        String result = map.get(KEY);
        assertNull(result);
    }

    @Test
    public void testCas() {
        CPMap<String, String> map = getMap();
        map.set(KEY, VALUE);
        assertTrue(map.compareAndSet(KEY, VALUE, "v2"));
        String result = map.get(KEY);
        assertNotNull(result);
        assertEquals("v2", result);
    }

    @Test
    public void testNullKey() {
        String expectedMessage = "argument 'key' cannot be null";

        // set
        Throwable actual = assertThrows(NullPointerException.class, () -> getMap().set(null, VALUE));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // put
        actual = assertThrows(NullPointerException.class, () -> getMap().put(null, VALUE));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // get
        actual = assertThrows(NullPointerException.class, () -> getMap().get(null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // remove
        actual = assertThrows(NullPointerException.class, () -> getMap().remove(null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // delete
        actual = assertThrows(NullPointerException.class, () -> getMap().delete(null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // compareAndSet
        actual = assertThrows(NullPointerException.class, () -> getMap().compareAndSet(null, VALUE, VALUE));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // putIfAbsent
        actual = assertThrows(NullPointerException.class, () -> getMap().putIfAbsent(null, VALUE));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());
    }

    @Test
    public void testNullValues() {
        String expectedMessage = "argument 'value' cannot be null";

        // set
        Throwable actual = assertThrows(NullPointerException.class, () -> getMap().set(KEY, null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // put
        actual = assertThrows(NullPointerException.class, () -> getMap().put(KEY, null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // putIfAbsent
        actual = assertThrows(NullPointerException.class, () -> getMap().putIfAbsent(KEY, null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        // compareAndSet
        expectedMessage = "argument 'expectedValue' cannot be null";
        actual = assertThrows(NullPointerException.class, () -> getMap().compareAndSet(KEY, null, VALUE));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());

        expectedMessage = "argument 'newValue' cannot be null";
        actual = assertThrows(NullPointerException.class, () -> getMap().compareAndSet(KEY, VALUE, null));
        assertNotNull(actual);
        assertEquals(expectedMessage, actual.getMessage());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        CPMap<String, String> map = getMap();
        map.destroy();
        map.set(KEY, VALUE);
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        CPMap<String, String> map = getMap();
        map.destroy();

        map = getMap();
        map.set(KEY, VALUE);
    }

    @Test
    public void testMultipleDestroy() {
        CPMap<String, String> map = getMap();
        map.destroy();
        map.destroy();
    }

    @After
    public void after() {
        client.shutdown();
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }
}
