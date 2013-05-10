package com.hazelcast.collection;


import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.client.*;
import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.collection.operations.client.AddAllRequest;
import com.hazelcast.collection.operations.client.ClearRequest;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @ali 5/10/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CollectionBinaryClientTest {


    static final String name = "test";
    static final CollectionProxyId mmProxyId = new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP);
    static final CollectionProxyId listProxyId = new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST);
    static Data dataKey = null;
    static HazelcastInstance hz = null;
    static SerializationService ss = null;
    Client c = null;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);

//        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
//        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        ss = new SerializationServiceImpl(0);
        dataKey = ss.toData(name);
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void startClient() throws IOException {
        c = new Client();
        c.auth();
    }

    @After
    public void closeClient() throws IOException {
        hz.getMultiMap(name).clear();
        hz.getList(name).clear();
        c.close();
    }

    @Test
    public void testAddAll() throws IOException {

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));
        list.add(ss.toData("item3"));
        list.add(ss.toData("item4"));



        c.send(new AddAllRequest(listProxyId, dataKey, getThreadId(), list));
        Object result = c.receive();
        assertTrue((Boolean) result);
        int size = hz.getList(name).size();
        assertEquals(size, list.size());

    }

    @Test
    public void testClear() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key1","value2");

        mm.put("key2","value3");

        c.send(new ClearRequest(mmProxyId));
        assertNull(c.receive());
        assertEquals(0, mm.size());
    }

    @Test
    public void testCompareAndRemove() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key1","value2");
        mm.put("key1","value3");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("value1"));
        list.add(ss.toData("value2"));

        c.send(new CompareAndRemoveRequest(mmProxyId, ss.toData("key1"),list, true, getThreadId()));
        Boolean result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals(2, mm.size());
        assertEquals(2, mm.valueCount("key1"));


        c.send(new CompareAndRemoveRequest(mmProxyId, ss.toData("key1"),list, false, getThreadId()));
        result = (Boolean)c.receive();
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

        c.send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
        boolean result = (Boolean)c.receive();
        assertTrue(result);

        dataSet.add(ss.toData("value"));
        c.send(new ContainsAllRequest(listProxyId, dataKey, dataSet));
        result = (Boolean)c.receive();
        assertFalse(result);
    }

    @Test
    public void testContainsEntry() throws IOException {
        MultiMap mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key2","value2");
        mm.put("key3","value3");

        //contains key value
        c.send(new ContainsEntryRequest(mmProxyId, ss.toData("key1"), ss.toData("value1")));
        boolean result = (Boolean)c.receive();
        assertTrue(result);

        //not contains key value
        c.send(new ContainsEntryRequest(mmProxyId, ss.toData("key1"), ss.toData("value2")));
        result = (Boolean)c.receive();
        assertFalse(result);

        //contains key
        c.send(new ContainsEntryRequest(mmProxyId, ss.toData("key2"), null));
        result = (Boolean)c.receive();
        assertTrue(result);

        //not contains key
        c.send(new ContainsEntryRequest(mmProxyId, ss.toData("key4"), null));
        result = (Boolean)c.receive();
        assertFalse(result);

        //contains value
        c.send(new ContainsEntryRequest(mmProxyId, null, ss.toData("value3")));
        result = (Boolean)c.receive();
        assertTrue(result);

        //not contains value
        c.send(new ContainsEntryRequest(mmProxyId, null, ss.toData("value0")));
        result = (Boolean)c.receive();
        assertFalse(result);
    }

    @Test
    public void testContains() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        c.send(new ContainsRequest(listProxyId, dataKey, ss.toData("value2")));
        boolean result = (Boolean)c.receive();
        assertTrue(result);

        c.send(new ContainsRequest(listProxyId, dataKey, ss.toData("value")));
        result = (Boolean)c.receive();
        assertFalse(result);
    }

    @Test
    public void testCount() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        c.send(new CountRequest(listProxyId, dataKey));
        int result = (Integer)c.receive();
        assertEquals(result, 3);
        assertEquals(result, list.size());

        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key1","value2");

        mm.put("key2","value2");

        c.send(new CountRequest(mmProxyId, ss.toData("key1")));
        result = (Integer)c.receive();
        assertEquals(result, 2);
        assertEquals(result, mm.valueCount("key1"));

        c.send(new CountRequest(mmProxyId, ss.toData("key2")));
        result = (Integer)c.receive();
        assertEquals(result, 1);
        assertEquals(result, mm.valueCount("key2"));
    }

    @Test
    public void testEntrySet() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key1","value2");

        mm.put("key2","value2");

        mm.put("key3","value3");

        mm.put("key4","value4");

        mm.put("key5","value3");

        c.send(new EntrySetRequest(mmProxyId));
        Set<Map.Entry> entrySet = (Set<Map.Entry>)c.receive();
        Data value = (Data)entrySet.iterator().next().getValue();
        String s = (String)ss.toObject(value);
        assertTrue(s.startsWith("value"));

        assertEquals(6, entrySet.size());

    }

    @Test
    public void testGetAll() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");

        mm.put("key2","value1");
        mm.put("key2","value2");


        c.send(new GetAllRequest(mmProxyId, ss.toData("key1")));
        CollectionResponse result = (CollectionResponse)c.receive();
        Collection<CollectionRecord> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject((Data) coll.iterator().next().getObject()));

        c.send(new GetAllRequest(mmProxyId, ss.toData("key2")));
        result = (CollectionResponse)c.receive();
        coll = result.getCollection();
        assertEquals(2, coll.size());
    }

    @Test
    public void testGet() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");

        c.send(new GetRequest(listProxyId, dataKey, 1));
        Object result = c.receive();
        assertEquals(result, "value2");

        c.send(new GetRequest(listProxyId, dataKey, 2));
        result = c.receive();
        assertEquals(result, "value3");
    }

    @Test
    public void testIndexOf() throws IOException {
        IList<Object> list = hz.getList(name);
        list.add("value1");
        list.add("value2");
        list.add("value3");
        list.add("value2");

        c.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), false));
        int result = (Integer)c.receive();
        assertEquals(1, result);

        c.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value2"), true));
        result = (Integer)c.receive();
        assertEquals(3, result);

        c.send(new IndexOfRequest(listProxyId, dataKey, ss.toData("value4"), false));
        result = (Integer)c.receive();
        assertEquals(-1, result);
    }

    @Test
    public void testKeySet() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");
        mm.put("key1","value2");
        mm.put("key1","value3");

        mm.put("key2","value1");
        mm.put("key2","value2");

        c.send(new KeySetRequest(mmProxyId));
        Set<Data> keySet = (Set<Data>)c.receive();
        assertEquals(2, keySet.size());

        String s = (String)ss.toObject(keySet.iterator().next());
        assertTrue(s.startsWith("key"));
    }

    @Test
    public void testPut() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);

        c.send(new PutRequest(mmProxyId, ss.toData("key1"),ss.toData("value1"),-1, getThreadId()));
        boolean result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals("value1", mm.get("key1").iterator().next());

        c.send(new PutRequest(mmProxyId, ss.toData("key1"),ss.toData("value1"),-1, getThreadId()));
        result = (Boolean)c.receive();
        assertFalse(result);

        assertEquals(1, mm.size());

        IList<Object> list = hz.getList(name);

        c.send(new PutRequest(listProxyId, dataKey, ss.toData("value3"),-1, getThreadId()));
        result = (Boolean)c.receive();
        assertTrue(result);

        c.send(new PutRequest(listProxyId, dataKey, ss.toData("value4"),-1, getThreadId()));
        result = (Boolean)c.receive();
        assertTrue(result);

        c.send(new PutRequest(listProxyId, dataKey, ss.toData("value5"),1, getThreadId()));
        result = (Boolean)c.receive();
        assertTrue(result);

        assertEquals(3, list.size());
        assertEquals("value3", list.get(0));
        assertEquals("value5", list.get(1));
        assertEquals("value4", list.get(2));
    }

    @Test
    public void testRemoveAll() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");

        mm.put("key2","value1");
        mm.put("key2","value2");


        c.send(new RemoveAllRequest(mmProxyId, ss.toData("key1"), getThreadId()));
        CollectionResponse result = (CollectionResponse)c.receive();
        Collection<CollectionRecord> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("value1", ss.toObject((Data) coll.iterator().next().getObject()));

        c.send(new RemoveAllRequest(mmProxyId, ss.toData("key2"), getThreadId()));
        result = (CollectionResponse)c.receive();
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

        c.send(new RemoveIndexRequest(listProxyId,dataKey, 0, getThreadId()));
        Object result = c.receive();
        assertEquals(result, "value1");
        assertEquals(2, list.size());

        c.send(new RemoveIndexRequest(listProxyId,dataKey, 1, getThreadId()));
        result = c.receive();
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

        c.send(new RemoveRequest(listProxyId,dataKey, ss.toData("value"), getThreadId()));
        boolean result = (Boolean)c.receive();
        assertFalse(result);
        assertEquals(3, list.size());

        c.send(new RemoveRequest(listProxyId,dataKey, ss.toData("value2"), getThreadId()));
        result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals(2, list.size());
    }

    @Test
    public void testSet() throws IOException {
        IList<Object> list = hz.getList(name);

        list.add("value1");
        list.add("value2");
        list.add("value3");

        c.send(new SetRequest(listProxyId, dataKey, ss.toData("value"), 1, getThreadId()));
        Object result = c.receive();
        assertEquals(result, "value2");
        assertEquals(list.size(), 3);
        assertEquals("value",list.get(1));
    }

    @Test
    public void testSize() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");

        mm.put("key2","value1");
        mm.put("key2","value2");

        mm.put("key3","value2");

        c.send(new SizeRequest(mmProxyId));
        int result = (Integer) c.receive();
        assertEquals(result, 4);
        assertEquals(result, mm.size());
    }

    @Test
    public void testValues() throws IOException {
        MultiMap<Object, Object> mm = hz.getMultiMap(name);
        mm.put("key1","value1");

        mm.put("key2","value1");
        mm.put("key2","value2");

        mm.put("key3","value2");

        c.send(new ValuesRequest(mmProxyId));
        Collection coll = (Collection)c.receive();
        assertEquals(4, coll.size());

        for (Object obj: coll){
            assertTrue(((String)obj).startsWith("value"));
        }
    }

    @Test
    public void testListener() throws IOException {
        c.send(new AddListenerRequest(mmProxyId, null, true, false));
        c.receive();

        hz.getMultiMap(name).put("key1","value1");

        String result = (String)c.receive();
        assertTrue(result.contains("value1"));
    }

    @Test
    public void testKeyListener() throws IOException {
        c.send(new AddListenerRequest(mmProxyId, ss.toData("key2"), true, false));
        c.receive();

        hz.getMultiMap(name).put("key1","value1");

        hz.getMultiMap(name).put("key2","value8");
        String result = (String)c.receive();
        assertFalse(result.contains("value1"));
        assertTrue(result.contains("value8"));
    }

    private int getThreadId(){
        int threadId = (int)Thread.currentThread().getId();
        return threadId;
    }

    static class Client {
        final Socket socket = new Socket();
        final SerializationService ss = new SerializationServiceImpl(0);
        final ObjectDataInputStream in;
        final ObjectDataOutputStream out;

        Client() throws IOException {
            socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5701));
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(new byte[]{'C', 'B', '1'});
            outputStream.flush();
            in = ss.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = ss.createObjectDataOutputStream(new BufferedOutputStream(outputStream));

            ClassDefinitionBuilder builder = new ClassDefinitionBuilder(ClientPortableHook.ID, ClientPortableHook.PRINCIPAL);
            builder.addUTFField("uuid").addUTFField("ownerUuid");
            ss.getSerializationContext().registerClassDefinition(builder.build());
        }

        void auth() throws IOException {
            AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
            send(auth);
            Object o = receive();
            System.err.println("AUTH -> " + o);
        }

        void send(Object o) throws IOException {
            final Data data = ss.toData(o);
            data.writeData(out);
            out.flush();
        }

        Object receive() throws IOException {
            Data responseData = new Data();
            responseData.readData(in);
            return ss.toObject(responseData);
        }

        void close() throws IOException {
            socket.close();
        }
    }

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
//        System.setProperty("java.net.preferIPv6Addresses", "true");
//        System.setProperty("hazelcast.prefer.ipv4.stack", "false");
    }
}
