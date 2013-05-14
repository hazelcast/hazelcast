/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.finalTest;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapValueCollection;
import com.hazelcast.map.client.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryDataResultStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapBinaryClientTest extends ClientTestSupport {

    static final String mapName = "test";
    static final SerializationService ss = new SerializationServiceImpl(0);
    static HazelcastInstance hz = null;
    static IMap map = null;


    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        map = hz.getMap(mapName);
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @After
    public void clear() throws IOException {
        map.clear();
    }

    @Test
    public void testPutGetSet() throws IOException {
        client().send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertNull(client().receive());
        client().send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, client().receive());
        client().send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(5, client().receive());
        client().send(new MapGetRequest(mapName, TestUtil.toData(7)));
        assertNull(client().receive());
        client().send(new MapSetRequest(mapName, TestUtil.toData(1), TestUtil.toData(7), ThreadContext.getThreadId()));
        System.out.println("-------" + client().receive());
        client().send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(7, client().receive());
        client().send(new MapPutTransientRequest(mapName, TestUtil.toData(1), TestUtil.toData(9), ThreadContext.getThreadId()));
        client().receive();
        client().send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(9, client().receive());
    }

    @Test
    public void testMapSize() throws IOException {
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put("-" + i, "-" + i);
        }
        client().send(new MapSizeRequest(mapName));
        assertEquals(size, client().receive());
    }

    @Test
    public void testMapKeyset() throws IOException {
        int size = 100;
        Set testSet = new HashSet();
        for (int i = 0; i < size; i++) {
            map.put(i, "v" + i);
            testSet.add(i);
        }
        client().send(new MapKeySetRequest(mapName));
        MapKeySet keyset = (MapKeySet) client().receive();
        for (Data o : keyset.getKeySet()) {
            Object x = TestUtil.toObject(o);
            assertTrue(testSet.remove(x));
        }
        assertEquals(0, testSet.size());
    }

    @Test
    public void testMapValues() throws IOException {
        int size = 100;
        Set testSet = new HashSet();
        for (int i = 0; i < size; i++) {
            map.put(i, "v" + i);
            testSet.add("v" + i);
        }
        client().send(new MapValuesRequest(mapName));
        MapValueCollection values = (MapValueCollection) client().receive();
        for (Data o : values.getValues()) {
            Object x = TestUtil.toObject(o);
            assertTrue(testSet.remove(x));
        }
        assertEquals(0, testSet.size());
    }

    @Test
    public void testMapContainsKeyValue() throws IOException {
        int size = 100;
        client().send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client().receive());
        for (int i = 0; i < size; i++) {
            map.put("-" + i, "-" + i);
        }
        for (int i = 0; i < size; i++) {
            client().send(new MapContainsKeyRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) client().receive());
        }
        client().send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client().receive());


        client().send(new MapContainsValueRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client().receive());
        for (int i = 0; i < size; i++) {
            client().send(new MapContainsValueRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) client().receive());
        }
        client().send(new MapContainsValueRequest(mapName, TestUtil.toData("--")));
        assertFalse((Boolean) client().receive());
    }

    @Test
    public void testMapRemoveDeleteEvict() throws IOException {
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < 100; i++) {
            client().send(new MapRemoveRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            assertEquals(i, client().receive());
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            client().send(new MapDeleteRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            client().receive();
            assertNull(map.get(i));
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            client().send(new MapEvictRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            client().receive();
            assertNull(map.get(i));
        }

        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame() throws IOException {
        map.put(1,5);
        client().send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertEquals(false, client().receive());
        client().send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(true, client().receive());
        assertEquals(0, map.size());
    }

    @Test
    public void testPutIfAbsent() throws IOException {
        map.put(1, 3);
        client().send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, client().receive());
        client().send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(2), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(null, client().receive());
        assertEquals(5, map.get(2));
    }

    @Test
    public void testMapReplace() throws IOException {
        map.put(1, 2);
        client().send(new MapReplaceRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertEquals(2, client().receive());
        assertEquals(3, map.get(1));
        client().send(new MapReplaceRequest(mapName, TestUtil.toData(2), TestUtil.toData(3), ThreadContext.getThreadId()));
        client().receive();
        assertEquals(null, map.get(2));
        client().send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(true, client().receive());
        assertEquals(5, map.get(1));
        client().send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(0), TestUtil.toData(7), ThreadContext.getThreadId()));
        assertEquals(false, client().receive());
        assertEquals(5, map.get(1));
    }

    @Test
    public void testMapTryPutRemove() throws IOException {
        client().send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId() + 1));
        client().receive();
        assertEquals(true, map.isLocked(1));
        client().send(new MapTryPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(1), ThreadContext.getThreadId(), 0));
        assertEquals(false, client().receive());
        client().send(new MapTryRemoveRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId(), 0));
        assertEquals(false, client().receive());
    }

    @Test
    public void testMapLockUnlock() throws IOException {
        client().send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId()));
        client().receive();
        assertEquals(true, map.isLocked(1));
        client().send(new MapUnlockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId()));
        client().receive();
        assertEquals(false, map.isLocked(1));
    }

    @Test
    public void testMapIsLocked() throws IOException {
        map.lock(1);
        client().send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(true, client().receive());
        map.unlock(1);
        client().send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(false, client().receive());
    }

    @Test
    public void testMapQuery() throws IOException {
        Set testSet = new HashSet();
        testSet.add("serra");
        testSet.add("met");
        map.put(1, new TestUtil.Employee("enes", 29, true, 100d));
        map.put(2, new TestUtil.Employee("serra", 3, true, 100d));
        map.put(3, new TestUtil.Employee("met", 7, true, 100d));
        MapQueryRequest request = new MapQueryRequest(mapName, new SqlPredicate("age < 10"), IterationType.VALUE);
        client().send(request);
        QueryDataResultStream result = (QueryDataResultStream) client().receive();
        Iterator iterator = result.iterator();
        while(iterator.hasNext()) {
            TestUtil.Employee x = (TestUtil.Employee) TestUtil.toObject((Data) iterator.next());
            testSet.remove(x.getName());
        }
        assertEquals(0, testSet.size());
    }

    @Test
    public void testVoid() throws IOException {
        client().send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, client().receive());
    }
}
