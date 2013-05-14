package com.hazelcast.collection;


import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.operations.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @ali 5/10/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CollectionBinaryClientTest extends ClientTestSupport {


    static final String name = "test";
    static final CollectionProxyId mmProxyId = new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP);
    static final CollectionProxyId listProxyId = new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST);
    static final SerializationService ss = new SerializationServiceImpl(0);
    static Data dataKey = null;
    static HazelcastInstance hz = null;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        dataKey = ss.toData(name);
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @After
    public void clear() throws IOException {
        hz.getMultiMap(name).clear();
        hz.getList(name).clear();
    }

    @Test
    public void testAddAll() throws IOException {

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));
        list.add(ss.toData("item3"));
        list.add(ss.toData("item4"));

        client().send(new AddAllRequest(listProxyId, dataKey, getThreadId(), list));
        Object result = client().receive();
        assertTrue((Boolean) result);
        int size = hz.getList(name).size();
        assertEquals(size, list.size());

    }

    @Test
    public void testClear() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key1", "value2");

        mm.put("key2", "value3");

        client().send(new ClearRequest(mmProxyId));
        assertNull(client().receive());
        assertEquals(0, mm.size());
    }

    @Test
    public void testCompareAndRemove() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key1", "value2");
        mm.put("key1", "value3");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("value1"));
        list.add(ss.toData("value2"));

        client().send(new CompareAndRemoveRequest(mmProxyId, ss.toData("key1"), list, true, getThreadId()));
        Boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(2, mm.size());
        assertEquals(2, mm.valueCount("key1"));


        client().send(new CompareAndRemoveRequest(mmProxyId, ss.toData("key1"), list, false, getThreadId()));
        result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(0, mm.size());
    }

    @Test
    public void testContainsAll() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        Set<Data> dataSet = new HashSet<Data>(2);
        dataSet.add(ss.toData("value2"));
        dataSet.add(ss.toData("value3"));

        client().send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
        boolean result = (Boolean) client().receive();
        assertTrue(result);

        dataSet.add(ss.toData("value"));
        client().send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
        result = (Boolean) client().receive();
        assertFalse(result);
    }

    @Test
    public void testContainsEntry() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key2", "value2");
        mm.put("key3", "value3");

        //contains key value
        client().send(new ContainsEntryRequest(mmProxyId, ss.toData("key1"), ss.toData("value1")));
        boolean result = (Boolean) client().receive();
        assertTrue(result);

        //not contains key value
        client().send(new ContainsEntryRequest(mmProxyId, ss.toData("key1"), ss.toData("value2")));
        result = (Boolean) client().receive();
        assertFalse(result);

        //contains key
        client().send(new ContainsEntryRequest(mmProxyId, ss.toData("key2"), null));
        result = (Boolean) client().receive();
        assertTrue(result);

        //not contains key
        client().send(new ContainsEntryRequest(mmProxyId, ss.toData("key4"), null));
        result = (Boolean) client().receive();
        assertFalse(result);

        //contains value
        client().send(new ContainsEntryRequest(mmProxyId, null, ss.toData("value3")));
        result = (Boolean) client().receive();
        assertTrue(result);

        //not contains value
        client().send(new ContainsEntryRequest(mmProxyId, null, ss.toData("value0")));
        result = (Boolean) client().receive();
        assertFalse(result);
    }

    @Test
    public void testContains() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new ContainsRequest(listProxyId, dataKey, ss.toData("value2")));
        boolean result = (Boolean) client().receive();
        assertTrue(result);

        client().send(new ContainsRequest(listProxyId, dataKey, ss.toData("value")));
        result = (Boolean) client().receive();
        assertFalse(result);
    }

    @Test
    public void testCount() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new CountRequest(listProxyId, dataKey));
        int result = (Integer) client().receive();
        assertEquals(result, 3);
        assertEquals(result, list.size());

        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key1", "value2");

        mm.put("key2", "value2");

        client().send(new CountRequest(mmProxyId, ss.toData("key1")));
        result = (Integer) client().receive();
        assertEquals(result, 2);
        assertEquals(result, mm.valueCount("key1"));

        client().send(new CountRequest(mmProxyId, ss.toData("key2")));
        result = (Integer) client().receive();
        assertEquals(result, 1);
        assertEquals(result, mm.valueCount("key2"));
    }

    @Test
    public void testEntrySet() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key1", "value2");

        mm.put("key2", "value2");

        mm.put("key3", "value3");

        mm.put("key4", "value4");

        mm.put("key5", "value3");

        client().send(new EntrySetRequest(mmProxyId));
        PortableEntrySetResponse result = (PortableEntrySetResponse) client().receive();
        Set<Map.Entry> entrySet = result.getEntrySet();
        Data value = (Data) entrySet.iterator().next().getValue();
        String s = (String) ss.toObject(value);
        assertTrue(s.startsWith("value"));

        assertEquals(6, entrySet.size());

    }

    @Test
    public void testGetAll() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");


        client().send(new GetAllRequest(mmProxyId, ss.toData("key1")));
        PortableCollectionResponse result = (PortableCollectionResponse) client().receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject(coll.iterator().next()));

        client().send(new GetAllRequest(mmProxyId, ss.toData("key2")));
        result = (PortableCollectionResponse) client().receive();
        coll = result.getCollection();
        assertEquals(2, coll.size());
    }

    @Test
    public void testGet() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new GetRequest(listProxyId, dataKey, 1));
        Object result = client().receive();
        assertEquals(result, "value2");

        client().send(new GetRequest(listProxyId, dataKey, 2));
        result = client().receive();
        assertEquals(result, "value3");
    }

    @Test
    public void testIndexOf() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");
        list.add("value2");

        client().send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), false));
        int result = (Integer) client().receive();
        assertEquals(1, result);

        client().send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), true));
        result = (Integer) client().receive();
        assertEquals(3, result);

        client().send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value4"), false));
        result = (Integer) client().receive();
        assertEquals(-1, result);
    }

    @Test
    public void testKeySet() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");
        mm.put("key1", "value2");
        mm.put("key1", "value3");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        client().send(new KeySetRequest(mmProxyId));
        PortableCollectionResponse result = (PortableCollectionResponse) client().receive();
        Collection<Data> keySet = result.getCollection();
        assertEquals(2, keySet.size());

        String s = (String) ss.toObject(keySet.iterator().next());
        assertTrue(s.startsWith("key"));
    }

    @Test
    public void testPut() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);

        client().send(new PutRequest(mmProxyId, ss.toData("key1"), ss.toData("value1"), -1, getThreadId()));
        boolean result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals("value1", mm.get("key1").iterator().next());

        client().send(new PutRequest(mmProxyId, ss.toData("key1"), ss.toData("value1"), -1, getThreadId()));
        result = (Boolean) client().receive();
        assertFalse(result);

        assertEquals(1, mm.size());

        IList<Object> list = hz.getList(name);

        client().send(new PutRequest(listProxyId, dataKey, ss.toData("value3"), -1, getThreadId()));
        result = (Boolean) client().receive();
        assertTrue(result);

        client().send(new PutRequest(listProxyId, dataKey, ss.toData("value4"), -1, getThreadId()));
        result = (Boolean) client().receive();
        assertTrue(result);

        client().send(new PutRequest(listProxyId, dataKey, ss.toData("value5"), 1, getThreadId()));
        result = (Boolean) client().receive();
        assertTrue(result);

        assertEquals(3, list.size());
        assertEquals("value3", list.get(0));
        assertEquals("value5", list.get(1));
        assertEquals("value4", list.get(2));
    }

    @Test
    public void testRemoveAll() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");


        client().send(new RemoveAllRequest(mmProxyId, ss.toData("key1"), getThreadId()));
        PortableCollectionResponse result = (PortableCollectionResponse) client().receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject(coll.iterator().next()));

        client().send(new RemoveAllRequest(mmProxyId, ss.toData("key2"), getThreadId()));
        result = (PortableCollectionResponse) client().receive();
        coll = result.getCollection();
        assertEquals(2, coll.size());

        assertEquals(0, mm.size());

    }

    @Test
    public void testRemoveIndex() throws IOException {
        IList<Object> list = hz.getList(name);

        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new RemoveIndexRequest(listProxyId, dataKey, 0, getThreadId()));
        Object result = client().receive();
        assertEquals(result, "value1");
        assertEquals(2, list.size());

        client().send(new RemoveIndexRequest(listProxyId, dataKey, 1, getThreadId()));
        result = client().receive();
        assertEquals(result, "value3");
        assertEquals(1, list.size());

        assertEquals("value2", list.get(0));
    }

    @Test
    public void testRemove() throws IOException {
        IList<Object> list = hz.getList(name);

        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new RemoveRequest(listProxyId, dataKey, ss.toData("value"), getThreadId()));
        boolean result = (Boolean) client().receive();
        assertFalse(result);
        assertEquals(3, list.size());

        client().send(new RemoveRequest(listProxyId, dataKey, ss.toData("value2"), getThreadId()));
        result = (Boolean) client().receive();
        assertTrue(result);
        assertEquals(2, list.size());
    }

    @Test
    public void testSet() throws IOException {
        IList<Object> list = hz.getList(name);

        list.add("value1");
        list.add("value2");
        list.add("value3");

        client().send(new SetRequest(listProxyId, dataKey, ss.toData("value"), 1, getThreadId()));
        Object result = client().receive();
        assertEquals(result, "value2");
        assertEquals(list.size(), 3);
        assertEquals("value", list.get(1));
    }

    @Test
    public void testSize() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        mm.put("key3", "value2");

        client().send(new SizeRequest(mmProxyId));
        int result = (Integer) client().receive();
        assertEquals(result, 4);
        assertEquals(result, mm.size());
    }

    @Test
    public void testValues() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1", "value1");

        mm.put("key2", "value1");
        mm.put("key2", "value2");

        mm.put("key3", "value2");

        client().send(new ValuesRequest(mmProxyId));
        PortableCollectionResponse result = (PortableCollectionResponse) client().receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(4, coll.size());

        for (Data data : coll) {
            Object obj = ss.toObject(data);
            assertTrue(((String) obj).startsWith("value"));
        }
    }

    @Test
    public void testListener() throws IOException {
        client().send(new AddListenerRequest(mmProxyId, null, true, false));
        client().receive();

        hz.getMultiMap(name).put("key1", "value1");

        String result = (String) client().receive();
        assertTrue(result.contains("value1"));
    }

    @Test
    public void testKeyListener() throws IOException {
        client().send(new AddListenerRequest(mmProxyId, ss.toData("key2"), true, false));
        client().receive();

        hz.getMultiMap(name).put("key1", "value1");

        hz.getMultiMap(name).put("key2", "value8");
        String result = (String) client().receive();
        assertFalse(result.contains("value1"));
        assertTrue(result.contains("value8"));
    }

    private int getThreadId() {
        int threadId = (int) Thread.currentThread().getId();
        return threadId;
    }
}
