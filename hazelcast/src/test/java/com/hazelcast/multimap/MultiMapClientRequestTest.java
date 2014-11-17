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

package com.hazelcast.multimap;


import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.MultiMap;
import com.hazelcast.multimap.impl.client.AddEntryListenerRequest;
import com.hazelcast.multimap.impl.client.ClearRequest;
import com.hazelcast.multimap.impl.client.ContainsRequest;
import com.hazelcast.multimap.impl.client.EntrySetRequest;
import com.hazelcast.multimap.impl.client.GetAllRequest;
import com.hazelcast.multimap.impl.client.KeyBasedContainsRequest;
import com.hazelcast.multimap.impl.client.KeySetRequest;
import com.hazelcast.multimap.impl.client.PortableEntrySetResponse;
import com.hazelcast.multimap.impl.client.PutRequest;
import com.hazelcast.multimap.impl.client.RemoveAllRequest;
import com.hazelcast.multimap.impl.client.SizeRequest;
import com.hazelcast.multimap.impl.client.ValuesRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author ali 5/10/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiMapClientRequestTest extends ClientTestSupport {

    static final String name = "test";
    static final SerializationService ss = new DefaultSerializationServiceBuilder().build();
    static final Data dataKey = ss.toData(name);

    protected Config createConfig() {
        return new Config();
    }

//    @Test
//    public void testAddAll() throws IOException {
//
//        List<Binary> list = new ArrayList<Binary>();
//        list.add(ss.toData("item1"));
//        list.add(ss.toData("item2"));
//        list.add(ss.toData("item3"));
//        list.add(ss.toData("item4"));
//
//        final SimpleClient client = getClient();
//        client.send(new AddAllRequest(listProxyId, dataKey, getThreadId(), list));
//        Object result = client.receive();
//        assertTrue((Boolean) result);
//        int size = getInstance().getList(name).size();
//        assertEquals(size, list.size());
//
//    }

    @Test
    public void testClear() throws IOException {
        MultiMap mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key1", "value2");

        mm.put("key2", "value3");

        final SimpleClient client = getClient();
        client.send(new ClearRequest(name));
        assertNull(client.receive());
        assertEquals(0, mm.size());
    }

//    @Test
//    public void testCompareAndRemove() throws IOException {
//        MultiMap mm = getMultiMap();
//        mm.put("key1", "value1");
//        mm.put("key1", "value2");
//        mm.put("key1", "value3");
//
//        List<Binary> list = new ArrayList<Binary>();
//        list.add(ss.toData("value1"));
//        list.add(ss.toData("value2"));
//
//        final SimpleClient client = getClient();
//        client.send(new CompareAndRemoveRequest(name, ss.toData("key1"), list, true, getThreadId()));
//        Boolean result = (Boolean) client.receive();
//        assertTrue(result);
//        assertEquals(2, mm.size());
//        assertEquals(2, mm.valueCount("key1"));
//
//
//        client.send(new CompareAndRemoveRequest(name, ss.toData("key1"), list, false, getThreadId()));
//        result = (Boolean) client.receive();
//        assertTrue(result);
//        assertEquals(0, mm.size());
//    }

//    @Test
//    public void testContainsAll() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        Set<Binary> dataSet = new HashSet<Binary>(2);
//        dataSet.add(ss.toData("value2"));
//        dataSet.add(ss.toData("value3"));
//
//        final SimpleClient client = getClient();
//        client.send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
//        boolean result = (Boolean) client.receive();
//        assertTrue(result);
//
//        dataSet.add(ss.toData("value"));
//        client.send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
//        result = (Boolean) client.receive();
//        assertFalse(result);
//    }

    @Test
    public void testContainsEntry() throws IOException {
        MultiMap mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key2", "value2");
        mm.put("key3", "value3");

        //contains key value
        final SimpleClient client = getClient();
        client.send(new KeyBasedContainsRequest(name, ss.toData("key1"), ss.toData("value1")));
        boolean result = (Boolean) client.receive();
        assertTrue(result);

        //not contains key value
        client.send(new KeyBasedContainsRequest(name, ss.toData("key1"), ss.toData("value2")));
        result = (Boolean) client.receive();
        assertFalse(result);

        //contains key
        client.send(new KeyBasedContainsRequest(name, ss.toData("key2"), null));
        result = (Boolean) client.receive();
        assertTrue(result);

        //not contains key
        client.send(new KeyBasedContainsRequest(name, ss.toData("key4"), null));
        result = (Boolean) client.receive();
        assertFalse(result);

        //contains value
        client.send(new ContainsRequest(name, ss.toData("value3")));
        result = (Boolean) client.receive();
        assertTrue(result);

        //not contains value
        client.send(new ContainsRequest(name, ss.toData("value0")));
        result = (Boolean) client.receive();
        assertFalse(result);
    }

//    @Test
//    public void testContains() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new ContainsRequest(listProxyId, dataKey, ss.toData("value2")));
//        boolean result = (Boolean) client.receive();
//        assertTrue(result);
//
//        client.send(new ContainsRequest(listProxyId, dataKey, ss.toData("value")));
//        result = (Boolean) client.receive();
//        assertFalse(result);
//    }

//    @Test
//    public void testCount() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new CountRequest(listProxyId, dataKey));
//        int result = (Integer) client.receive();
//        assertEquals(result, 3);
//        assertEquals(result, list.size());
//
//        MultiMap<Object, Object> mm = getMultiMap();
//        mm.put("key1", "value1");
//        mm.put("key1", "value2");
//
//        mm.put("key2", "value2");
//
//        client.send(new CountRequest(name, ss.toData("key1")));
//        result = (Integer) client.receive();
//        assertEquals(result, 2);
//        assertEquals(result, mm.valueCount("key1"));
//
//        client.send(new CountRequest(name, ss.toData("key2")));
//        result = (Integer) client.receive();
//        assertEquals(result, 1);
//        assertEquals(result, mm.valueCount("key2"));
//    }

    @Test
    public void testEntrySet() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key1", "value2");

        mm.put("key2", "value2");

        mm.put("key3", "value3");

        mm.put("key4", "value4");

        mm.put("key5", "value3");

        final SimpleClient client = getClient();
        client.send(new EntrySetRequest(name));
        PortableEntrySetResponse result = (PortableEntrySetResponse) client.receive();
        Set<Map.Entry> entrySet = result.getEntrySet();
        Data value = (Data) entrySet.iterator().next().getValue();
        String s = (String) ss.toObject(value);
        assertTrue(s.startsWith("value"));
        assertEquals(6, entrySet.size());
    }

    @Test
    public void testGetAll() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        final SimpleClient client = getClient();
        client.send(new GetAllRequest(name, ss.toData("key1")));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject(coll.iterator().next()));

        client.send(new GetAllRequest(name, ss.toData("key2")));
        result = (PortableCollection) client.receive();
        coll = result.getCollection();
        assertEquals(2, coll.size());
    }

//    @Test
//    public void testGet() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new GetRequest(listProxyId, dataKey, 1));
//        Object result = client.receive();
//        assertEquals(result, "value2");
//
//        client.send(new GetRequest(listProxyId, dataKey, 2));
//        result = client.receive();
//        assertEquals(result, "value3");
//    }

//    @Test
//    public void testIndexOf() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//        list.add("value2");
//
//        final SimpleClient client = getClient();
//        client.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), false));
//        int result = (Integer) client.receive();
//        assertEquals(1, result);
//
//        client.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), true));
//        result = (Integer) client.receive();
//        assertEquals(3, result);
//
//        client.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value4"), false));
//        result = (Integer) client.receive();
//        assertEquals(-1, result);
//    }

    @Test
    public void testKeySet() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key1", "value2");
        mm.put("key1", "value3");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        final SimpleClient client = getClient();
        client.send(new KeySetRequest(name));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> keySet = result.getCollection();
        assertEquals(2, keySet.size());

        String s = (String) ss.toObject(keySet.iterator().next());
        assertTrue(s.startsWith("key"));
    }

    @Test
    public void testPut() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();

        final SimpleClient client = getClient();
        client.send(new PutRequest(name, ss.toData("key1"), ss.toData("value1"), -1, getThreadId()));
        boolean result = (Boolean) client.receive();
        assertTrue(result);
        assertEquals("value1", mm.get("key1").iterator().next());

        client.send(new PutRequest(name, ss.toData("key1"), ss.toData("value1"), -1, getThreadId()));
        result = (Boolean) client.receive();
        assertFalse(result);

        assertEquals(1, mm.size());

//        IList<Object> list = getInstance().getList(name);
//
//        client.send(new PutRequest(listProxyId, dataKey, ss.toData("value3"), -1, getThreadId()));
//        result = (Boolean) client.receive();
//        assertTrue(result);
//
//        client.send(new PutRequest(listProxyId, dataKey, ss.toData("value4"), -1, getThreadId()));
//        result = (Boolean) client.receive();
//        assertTrue(result);
//
//        client.send(new PutRequest(listProxyId, dataKey, ss.toData("value5"), 1, getThreadId()));
//        result = (Boolean) client.receive();
//        assertTrue(result);
//
//        assertEquals(3, list.size());
//        assertEquals("value3", list.get(0));
//        assertEquals("value5", list.get(1));
//        assertEquals("value4", list.get(2));
    }

    @Test
    public void testRemoveAll() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        final SimpleClient client = getClient();
        client.send(new RemoveAllRequest(name, ss.toData("key1"), getThreadId()));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject(coll.iterator().next()));

        client.send(new RemoveAllRequest(name, ss.toData("key2"), getThreadId()));
        result = (PortableCollection) client.receive();
        coll = result.getCollection();
        assertEquals(2, coll.size());
        assertEquals(0, mm.size());
    }

//    @Test
//    public void testRemoveIndex() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new RemoveIndexRequest(listProxyId, dataKey, 0, getThreadId()));
//        Object result = client.receive();
//        assertEquals(result, "value1");
//        assertEquals(2, list.size());
//
//        client.send(new RemoveIndexRequest(listProxyId, dataKey, 1, getThreadId()));
//        result = client.receive();
//        assertEquals(result, "value3");
//        assertEquals(1, list.size());
//
//        assertEquals("value2", list.get(0));
//    }

//    @Test
//    public void testRemove() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new RemoveRequest(listProxyId, dataKey, ss.toData("value"), getThreadId()));
//        boolean result = (Boolean) client.receive();
//        assertFalse(result);
//        assertEquals(3, list.size());
//
//        client.send(new RemoveRequest(listProxyId, dataKey, ss.toData("value2"), getThreadId()));
//        result = (Boolean) client.receive();
//        assertTrue(result);
//        assertEquals(2, list.size());
//    }

//    @Test
//    public void testSet() throws IOException {
//        IList<Object> list = getInstance().getList(name);
//        list.add("value1");
//        list.add("value2");
//        list.add("value3");
//
//        final SimpleClient client = getClient();
//        client.send(new SetRequest(listProxyId, dataKey, ss.toData("value"), 1, getThreadId()));
//        Object result = client.receive();
//        assertEquals(result, "value2");
//        assertEquals(list.size(), 3);
//        assertEquals("value", list.get(1));
//    }

    @Test
    public void testSize() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key2", "value1");
        mm.put("key2", "value2");
        mm.put("key3", "value2");

        final SimpleClient client = getClient();
        client.send(new SizeRequest(name));
        int result = (Integer) client.receive();
        assertEquals(result, 4);
        assertEquals(result, mm.size());
    }

    @Test
    public void testValues() throws IOException {
        MultiMap<Object, Object> mm = getMultiMap();
        mm.put("key1", "value1");
        mm.put("key2", "value1");
        mm.put("key2", "value2");
        mm.put("key3", "value2");

        final SimpleClient client = getClient();
        client.send(new ValuesRequest(name));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(4, coll.size());

        for (Data data : coll) {
            Object obj = ss.toObject(data);
            assertTrue(((String) obj).startsWith("value"));
        }
    }

    @Test
    public void testListener() throws IOException {
        final SimpleClient client = getClient();
        client.send(new AddEntryListenerRequest(name, null, true));
        client.receive();

        getMultiMap().put("key1", "value1");

        PortableEntryEvent result = (PortableEntryEvent) client.receive();
        assertEquals("key1", ss.toObject(result.getKey()));
        assertEquals("value1", ss.toObject(result.getValue()));
    }

    @Test
    public void testKeyListener() throws IOException {
        final SimpleClient client = getClient();
        client.send(new AddEntryListenerRequest(name, ss.toData("key2"), true));
        client.receive();

        final MultiMap<Object, Object> multiMap = getMultiMap();
        multiMap.put("key1", "value1");
        multiMap.put("key2", "value8");

        PortableEntryEvent result = (PortableEntryEvent) client.receive();
        assertEquals("key2", ss.toObject(result.getKey()));
        assertEquals("value8", ss.toObject(result.getValue()));
    }

    private MultiMap<Object, Object> getMultiMap() {
        return getInstance().getMultiMap(name);
    }

    private long getThreadId() {
        return Thread.currentThread().getId();
    }
}
