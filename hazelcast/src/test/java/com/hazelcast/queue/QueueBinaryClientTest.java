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

package com.hazelcast.queue;

import com.hazelcast.clientv2.AuthenticationRequest;
import com.hazelcast.clientv2.ClientPortableHook;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.queue.clientv2.*;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @ali 5/8/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class QueueBinaryClientTest {

    static final String queueName = "test";
    static HazelcastInstance hz = null;
    static SerializationService ss = null;
    Client c = null;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        QueueConfig queueConfig = hz.getConfig().getQueueConfig(queueName);
        queueConfig.setMaxSize(6);
        ss = new SerializationServiceImpl(0);
    }

    @AfterClass
    public static void destroy(){
        Hazelcast.shutdownAll();
    }

    @Before
    public void startClient() throws IOException {
        c = new Client();
        c.auth();
    }

    @After
    public void closeClient() throws IOException {
        hz.getQueue(queueName).clear();
        c.close();
    }

    @Test
    public void testAddAll() throws IOException {
        
        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));
        list.add(ss.toData("item3"));
        list.add(ss.toData("item4"));
        c.send(new AddAllRequest(queueName, list));
        Object result = c.receive();
        assertTrue((Boolean)result);
        int size = hz.getQueue(queueName).size();
        assertEquals(size, list.size());
        
    }

    @Test
    public void testAddListener() throws IOException {

        

        c.send(new AddListenerRequest(queueName, true));
        c.receive();

        hz.getQueue(queueName).offer("item");


        String result = (String)c.receive();
        assertTrue(result.startsWith("ItemEvent"));

        

    }

    @Test
    public void testClear() throws Exception {

        

        IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        c.send(new ClearRequest(queueName));
        Object result = c.receive();
        assertTrue((Boolean)result);
        assertEquals(0, q.size());

        
    }

    @Test
    public void testCompareAndRemove() throws IOException {
        

        IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));

        c.send(new CompareAndRemoveRequest(queueName, list, true));
        Boolean result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals(2, q.size());


        c.send(new CompareAndRemoveRequest(queueName, list, false));
        result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals(0, q.size());

        
    }

    @Test
    public void testContains() throws IOException {
        

        IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));

        c.send(new ContainsRequest(queueName, list));
        Boolean result = (Boolean)c.receive();
        assertTrue(result);

        list.add(ss.toData("item0"));

        c.send(new ContainsRequest(queueName, list));
        result = (Boolean)c.receive();
        assertFalse(result);

        
    }

    @Test
    public void testDrain() throws IOException {
        

        IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        c.send(new DrainRequest(queueName, 1));
        SerializableCollectionContainer result = (SerializableCollectionContainer)c.receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("item1", ss.toObject(coll.iterator().next()));
        assertEquals(4, q.size());
        
    }

    @Test
    public void testIterator() throws IOException {
        
        IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        c.send(new IteratorRequest(queueName));
        SerializableCollectionContainer result = (SerializableCollectionContainer)c.receive();
        Collection<Data> coll = result.getCollection();
        int i=1;
        for (Data data: coll){
            assertEquals("item"+i,ss.toObject(data));
            i++;
        }
        
    }

    @Test
    public void testOffer() throws IOException {
        

        final IQueue q = hz.getQueue(queueName);

        c.send(new OfferRequest(queueName, ss.toData("item1")));
        Object result = c.receive();
        assertTrue((Boolean)result);
        Object item = q.peek();
        assertEquals(item, "item1");


        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");
        q.offer("item6");

        new Thread(){
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        }.start();
        c.send(new OfferRequest(queueName, 500, ss.toData("item7")));
        result = c.receive();
        assertFalse((Boolean)result);

        c.send(new OfferRequest(queueName, 10*1000, ss.toData("item7")));
        result = c.receive();
        assertTrue((Boolean) result);

        

    }

    @Test
    public void testPeek() throws IOException {
        
        IQueue q = hz.getQueue(queueName);

        c.send(new PeekRequest(queueName));
        Object result = c.receive();
        assertNull(result);

        q.offer("item1");
        c.send(new PeekRequest(queueName));
        result = c.receive();
        assertEquals("item1", result);
        assertEquals(1, q.size());
        
    }

    @Test
    public void testPoll() throws IOException {
        

        final IQueue q = hz.getQueue(queueName);

        c.send(new PollRequest(queueName));
        Object result = c.receive();
        assertNull(result);

        q.offer("item1");
        c.send(new PollRequest(queueName));
        result = c.receive();
        assertEquals("item1", result);
        assertEquals(0, q.size());


        new Thread(){
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("item2");
            }
        }.start();
        c.send(new PollRequest(queueName, 10*1000));
        result = c.receive();
        assertEquals("item2", result);
        assertEquals(0, q.size());
        

    }

    @Test
    public void testRemove() throws IOException {
        
        final IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        c.send(new RemoveRequest(queueName, ss.toData("item2")));
        Boolean result = (Boolean)c.receive();
        assertTrue(result);
        assertEquals(2,q.size());

        c.send(new RemoveRequest(queueName, ss.toData("item2")));
        result = (Boolean)c.receive();
        assertFalse(result);
        assertEquals(2,q.size());
        
    }

    @Test
    public void testSize() throws IOException {
        
        final IQueue q = hz.getQueue(queueName);
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        c.send(new SizeRequest(queueName));
        int result = (Integer)c.receive();
        assertEquals(result, q.size());
        
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
