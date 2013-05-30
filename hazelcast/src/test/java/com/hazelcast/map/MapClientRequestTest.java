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

package com.hazelcast.map;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.client.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryDataResultStream;
import com.hazelcast.util.ThreadUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class MapClientRequestTest extends ClientTestSupport {

    static final String mapName = "test";

    protected Config createConfig() {
        return new Config();
    }

    private IMap getMap() {
        return getInstance().getMap(mapName);
    }

    @Test
    public void testPutGetSet() throws IOException {
        final SimpleClient client = getClient();
        client.send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadUtil.getThreadId()));
        assertNull(client.receive());
        client.send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadUtil.getThreadId()));
        assertEquals(3, client.receive());
        client.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(5, client.receive());
        client.send(new MapGetRequest(mapName, TestUtil.toData(7)));
        assertNull(client.receive());
        client.send(new MapSetRequest(mapName, TestUtil.toData(1), TestUtil.toData(7), ThreadUtil.getThreadId()));
        assertNull(client.receive());
        client.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(7, client.receive());
        client.send(new MapPutTransientRequest(mapName, TestUtil.toData(1), TestUtil.toData(9), ThreadUtil.getThreadId()));
        client.receive();
        client.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(9, client.receive());
    }

    @Test
    public void testMapSize() throws IOException {
        int size = 100;
        final IMap map = getMap();
        for (int i = 0; i < size; i++) {
            map.put("-" + i, "-" + i);
        }
        final SimpleClient client = getClient();
        client.send(new MapSizeRequest(mapName));
        assertEquals(size, client.receive());
    }

    @Test
    public void testMapKeyset() throws IOException {
        int size = 100;
        Set testSet = new HashSet();
        final IMap map = getMap();
        for (int i = 0; i < size; i++) {
            map.put(i, "v" + i);
            testSet.add(i);
        }
        final SimpleClient client = getClient();
        client.send(new MapKeySetRequest(mapName));
        MapKeySet keyset = (MapKeySet) client.receive();
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
        final IMap map = getMap();
        for (int i = 0; i < size; i++) {
            map.put(i, "v" + i);
            testSet.add("v" + i);
        }
        final SimpleClient client = getClient();
        client.send(new MapValuesRequest(mapName));
        MapValueCollection values = (MapValueCollection) client.receive();
        for (Data o : values.getValues()) {
            Object x = TestUtil.toObject(o);
            assertTrue(testSet.remove(x));
        }
        assertEquals(0, testSet.size());
    }

    @Test
    public void testMapContainsKeyValue() throws IOException {
        int size = 100;
        final SimpleClient client = getClient();
        client.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client.receive());
        for (int i = 0; i < size; i++) {
            getMap().put("-" + i, "-" + i);
        }
        for (int i = 0; i < size; i++) {
            client.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) client.receive());
        }
        client.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client.receive());


        client.send(new MapContainsValueRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) client.receive());
        for (int i = 0; i < size; i++) {
            client.send(new MapContainsValueRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) client.receive());
        }
        client.send(new MapContainsValueRequest(mapName, TestUtil.toData("--")));
        assertFalse((Boolean) client.receive());
    }

    @Test
    public void testMapRemoveDeleteEvict() throws IOException {
        final IMap map = getMap();
        final SimpleClient client = getClient();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < 100; i++) {
            getClient().send(new MapRemoveRequest(mapName, TestUtil.toData(i), ThreadUtil.getThreadId()));
            assertEquals(i, client.receive());
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            client.send(new MapDeleteRequest(mapName, TestUtil.toData(i), ThreadUtil.getThreadId()));
            client.receive();
            assertNull(map.get(i));
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            client.send(new MapEvictRequest(mapName, TestUtil.toData(i), ThreadUtil.getThreadId()));
            client.receive();
            assertNull(map.get(i));
        }

        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame() throws IOException {
        getMap().put(1, 5);
        final SimpleClient client = getClient();
        client.send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadUtil.getThreadId()));
        assertEquals(false, client.receive());
        client.send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadUtil.getThreadId()));
        assertEquals(true, client.receive());
        assertEquals(0, getMap().size());
    }

    @Test
    public void testPutIfAbsent() throws IOException {
        final IMap map = getMap();
        map.put(1, 3);
        final SimpleClient client = getClient();
        client.send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadUtil.getThreadId()));
        assertEquals(3, client.receive());
        client.send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(2), TestUtil.toData(5), ThreadUtil.getThreadId()));
        assertEquals(null, client.receive());
        assertEquals(5, map.get(2));
    }

    @Test
    public void testMapReplace() throws IOException {
        final IMap map = getMap();
        map.put(1, 2);
        final SimpleClient client = getClient();
        client.send(new MapReplaceRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadUtil.getThreadId()));
        assertEquals(2, client.receive());
        assertEquals(3, map.get(1));
        client.send(new MapReplaceRequest(mapName, TestUtil.toData(2), TestUtil.toData(3), ThreadUtil.getThreadId()));
        client.receive();
        assertEquals(null, map.get(2));
        client.send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), TestUtil.toData(5), ThreadUtil.getThreadId()));
        assertEquals(true, client.receive());
        assertEquals(5, map.get(1));
        client.send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(0), TestUtil.toData(7), ThreadUtil.getThreadId()));
        assertEquals(false, client.receive());
        assertEquals(5, map.get(1));
    }

    @Test
    public void testMapTryPutRemove() throws IOException {
        final SimpleClient client = getClient();
        client.send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadUtil.getThreadId() + 1));
        client.receive();
        assertEquals(true, getMap().isLocked(1));
        client.send(new MapTryPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(1), ThreadUtil.getThreadId(), 0));
        assertEquals(false, client.receive());
        client.send(new MapTryRemoveRequest(mapName, TestUtil.toData(1), ThreadUtil.getThreadId(), 0));
        assertEquals(false, client.receive());
    }

    @Test
    public void testMapLockUnlock() throws IOException {
        final SimpleClient client = getClient();
        client.send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadUtil.getThreadId()));
        client.receive();
        final IMap map = getMap();
        assertEquals(true, map.isLocked(1));
        client.send(new MapUnlockRequest(mapName, TestUtil.toData(1), ThreadUtil.getThreadId()));
        client.receive();
        assertEquals(false, map.isLocked(1));
    }

    @Test
    public void testMapIsLocked() throws IOException {
        final IMap map = getMap();
        map.lock(1);
        final SimpleClient client = getClient();
        client.send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(true, client.receive());
        map.unlock(1);
        client.send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(false, client.receive());
    }

    @Test
    public void testMapQuery() throws IOException {
        Set testSet = new HashSet();
        testSet.add("serra");
        testSet.add("met");
        final IMap map = getMap();
        map.put(1, new TestUtil.Employee("enes", 29, true, 100d));
        map.put(2, new TestUtil.Employee("serra", 3, true, 100d));
        map.put(3, new TestUtil.Employee("met", 7, true, 100d));
        MapQueryRequest request = new MapQueryRequest(mapName, new SqlPredicate("age < 10"), IterationType.VALUE);
        final SimpleClient client = getClient();
        client.send(request);
        QueryDataResultStream result = (QueryDataResultStream) client.receive();
        Iterator iterator = result.iterator();
        while(iterator.hasNext()) {
            TestUtil.Employee x = (TestUtil.Employee) TestUtil.toObject((Data) iterator.next());
            testSet.remove(x.getName());
        }
        assertEquals(0, testSet.size());
    }
}
