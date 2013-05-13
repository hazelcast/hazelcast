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

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.collection.operations.client.AddListenerRequest;
import com.hazelcast.collection.operations.client.CompareAndRemoveRequest;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.client.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.queue.SerializableCollectionContainer;
import com.hazelcast.queue.client.ClearRequest;
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
import static org.junit.Assert.assertEquals;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapBinaryClientTest {

    static final String mapName = "test";
    static HazelcastInstance hz = null;
    static SerializationService ss = null;
    static IMap map = null;
    Client c = null;


    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        ss = new SerializationServiceImpl(0);
        map = hz.getMap(mapName);
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
        map.clear();
        c.close();
    }

    @Test
    public void testPutGetSet() throws IOException {
        c.send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertNull(c.receive());
        c.send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, c.receive());
        c.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(5, c.receive());
        c.send(new MapGetRequest(mapName, TestUtil.toData(7)));
        assertNull(c.receive());
        c.send(new MapSetRequest(mapName, TestUtil.toData(1), TestUtil.toData(7), ThreadContext.getThreadId()));
        System.out.println("-------" + c.receive());
        c.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(7, c.receive());
        c.send(new MapPutTransientRequest(mapName, TestUtil.toData(1), TestUtil.toData(9), ThreadContext.getThreadId()));
        c.receive();
        c.send(new MapGetRequest(mapName, TestUtil.toData(1)));
        assertEquals(9, c.receive());
    }

    @Test
    public void testMapSize() throws IOException {
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put("-" + i, "-" + i);
        }
        c.send(new MapSizeRequest(mapName));
        assertEquals(size, c.receive());
    }

    @Test
    public void testMapKeyset() throws IOException {
        int size = 100;
        Set testSet = new HashSet();
        for (int i = 0; i < size; i++) {
            map.put(i, "v" + i);
            testSet.add(i);
        }
        c.send(new MapKeySetRequest(mapName));
        MapKeySet keyset = (MapKeySet) c.receive();
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
        c.send(new MapValuesRequest(mapName));
        MapValueCollection values = (MapValueCollection) c.receive();
        for (Data o : values.getValues()) {
            Object x = TestUtil.toObject(o);
            assertTrue(testSet.remove(x));
        }
        assertEquals(0, testSet.size());
    }

    @Test
    public void testMapContainsKeyValue() throws IOException {
        int size = 100;
        c.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) c.receive());
        for (int i = 0; i < size; i++) {
            map.put("-" + i, "-" + i);
        }
        for (int i = 0; i < size; i++) {
            c.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) c.receive());
        }
        c.send(new MapContainsKeyRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) c.receive());


        c.send(new MapContainsValueRequest(mapName, TestUtil.toData("-")));
        assertFalse((Boolean) c.receive());
        for (int i = 0; i < size; i++) {
            c.send(new MapContainsValueRequest(mapName, TestUtil.toData("-" + i)));
            assertTrue((Boolean) c.receive());
        }
        c.send(new MapContainsValueRequest(mapName, TestUtil.toData("--")));
        assertFalse((Boolean) c.receive());
    }

    @Test
    public void testMapRemoveDeleteEvict() throws IOException {
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < 100; i++) {
            c.send(new MapRemoveRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            assertEquals(i, c.receive());
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            c.send(new MapDeleteRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            c.receive();
            assertNull(map.get(i));
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            c.send(new MapEvictRequest(mapName, TestUtil.toData(i), ThreadContext.getThreadId()));
            c.receive();
            assertNull(map.get(i));
        }

        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame() throws IOException {
        map.put(1,5);
        c.send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertEquals(false, c.receive());
        c.send(new MapRemoveIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(true, c.receive());
        assertEquals(0, map.size());
    }

    @Test
    public void testPutIfAbsent() throws IOException {
        map.put(1, 3);
        c.send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, c.receive());
        c.send(new MapPutIfAbsentRequest(mapName, TestUtil.toData(2), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(null, c.receive());
        assertEquals(5, map.get(2));
    }

    @Test
    public void testMapReplace() throws IOException {
        map.put(1, 2);
        c.send(new MapReplaceRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), ThreadContext.getThreadId()));
        assertEquals(2, c.receive());
        assertEquals(3, map.get(1));
        c.send(new MapReplaceRequest(mapName, TestUtil.toData(2), TestUtil.toData(3), ThreadContext.getThreadId()));
        c.receive();
        assertEquals(null, map.get(2));
        c.send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(3), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(true, c.receive());
        assertEquals(5, map.get(1));
        c.send(new MapReplaceIfSameRequest(mapName, TestUtil.toData(1), TestUtil.toData(0), TestUtil.toData(7), ThreadContext.getThreadId()));
        assertEquals(false, c.receive());
        assertEquals(5, map.get(1));
    }

    @Test
    public void testMapTryPutRemove() throws IOException {
        c.send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId() + 1));
        c.receive();
        assertEquals(true, map.isLocked(1));
        c.send(new MapTryPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(1), ThreadContext.getThreadId(), 0));
        assertEquals(false, c.receive());
        c.send(new MapTryRemoveRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId(), 0));
        assertEquals(false, c.receive());
    }

    @Test
    public void testMapLockUnlock() throws IOException {
        c.send(new MapLockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId()));
        c.receive();
        assertEquals(true, map.isLocked(1));
        c.send(new MapUnlockRequest(mapName, TestUtil.toData(1), ThreadContext.getThreadId()));
        c.receive();
        assertEquals(false, map.isLocked(1));
    }

    @Test
    public void testMapIsLocked() throws IOException {
        map.lock(1);
        c.send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(true, c.receive());
        map.unlock(1);
        c.send(new MapIsLockedRequest(mapName, TestUtil.toData(1)));
        assertEquals(false, c.receive());
    }

    @Test
    public void testVoid() throws IOException {
        c.send(new MapPutRequest(mapName, TestUtil.toData(1), TestUtil.toData(5), ThreadContext.getThreadId()));
        assertEquals(3, c.receive());
    }

    @Test
    public void testClear() throws Exception {

        IQueue q = hz.getQueue(mapName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        c.send(new ClearRequest(mapName));
        Object result = c.receive();
        assertTrue((Boolean) result);
        assertEquals(0, q.size());


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
