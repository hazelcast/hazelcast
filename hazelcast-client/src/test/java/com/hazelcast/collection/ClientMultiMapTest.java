/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import org.junit.*;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/20/13
 */
public class ClientMultiMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static MultiMap mm;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        MultiMapConfig multiMapConfig = config.getMultiMapConfig(name);
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        mm = hz.getMultiMap(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        mm.clear();
    }

    @Test
    public void testPutGetRemove() {
        assertTrue(mm.put("key1","value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));

        assertEquals(3, mm.valueCount("key1"));
        assertEquals(2, mm.valueCount("key2"));
        assertEquals(5, mm.size());

        Collection coll = mm.get("key1");
        assertEquals(3, coll.size());

        coll = mm.remove("key2");
        assertEquals(2, coll.size());
        assertEquals(0, mm.valueCount("key2"));
        assertEquals(0, mm.get("key2").size());

        assertFalse(mm.remove("key1", "value4"));
        assertEquals(3, mm.size());

        assertTrue(mm.remove("key1", "value2"));
        assertEquals(2, mm.size());

        assertTrue(mm.remove("key1", "value1"));
        assertEquals(1, mm.size());
        assertEquals("value3",mm.get("key1").iterator().next());
    }

    @Test
    public void testKeySetEntrySetAndValues(){
        assertTrue(mm.put("key1","value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));


        assertEquals(2, mm.keySet().size());
        assertEquals(5, mm.values().size());
        assertEquals(5, mm.entrySet().size());
    }

    @Test
    public void testContains(){
        assertTrue(mm.put("key1","value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));

        assertFalse(mm.containsKey("key3"));
        assertTrue(mm.containsKey("key1"));

        assertFalse(mm.containsValue("value6"));
        assertTrue(mm.containsValue("value4"));

        assertFalse(mm.containsEntry("key1","value4"));
        assertFalse(mm.containsEntry("key2","value3"));
        assertTrue(mm.containsEntry("key1","value1"));
        assertTrue(mm.containsEntry("key2","value5"));
    }
}
