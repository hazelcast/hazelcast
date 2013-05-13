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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.concurrent.semaphore.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
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

import static org.junit.Assert.*;

/**
 * @ali 5/13/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SemaphoreBinaryClientTest {

    static final String name = "test";
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
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void startClient() throws IOException {
        c = new Client();
        c.auth();
        ISemaphore s = hz.getSemaphore(name);
        s.reducePermits(100);
        assertEquals(0, s.availablePermits());
    }

    @After
    public void closeClient() throws IOException {
        ISemaphore s = hz.getSemaphore(name);
        s.reducePermits(100);
        assertEquals(0, s.availablePermits());
        c.close();
    }

    @Test
    public void testAcquire() throws Exception {

        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        c.send(new AcquireRequest(name, 3, 0));
        boolean result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(7, s.availablePermits());

        c.send(new AcquireRequest(name, 8, 6*1000));
        assertEquals(7, s.availablePermits());

        Thread.sleep(2*1000);

        s.release(1);

        result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(0, s.availablePermits());

        c.send(new AcquireRequest(name, 4, 2*1000));
        result = (Boolean) c.receive();
        assertFalse(result);

    }

    @Test
    public void testAvailable() throws Exception {
        c.send(new AvailableRequest(name));
        int result = (Integer)c.receive();
        assertEquals(0, result);

        ISemaphore s = hz.getSemaphore(name);
        s.release(5);

        c.send(new AvailableRequest(name));
        result = (Integer)c.receive();
        assertEquals(5, result);
    }

    @Test
    public void testDrain() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        c.send(new DrainRequest(name));
        int result = (Integer) c.receive();
        assertEquals(10, result);


        s.release(4);

        c.send(new DrainRequest(name));
        result = (Integer) c.receive();
        assertEquals(4, result);
    }

    @Test
    public void testInit() throws Exception{
        ISemaphore s = hz.getSemaphore(name);

        c.send(new InitRequest(name, 10));
        boolean result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(10, s.availablePermits());

        c.send(new InitRequest(name, 20));
        result = (Boolean) c.receive();
        assertFalse(result);
        assertEquals(10, s.availablePermits());
    }

    @Test
    public void testReduce() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        c.send(new ReduceRequest(name, 4));
        boolean result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(6, s.availablePermits());
    }

    @Test
    public void testRelease() throws Exception {
        ISemaphore s = hz.getSemaphore(name);
        assertTrue(s.init(10));

        c.send(new ReleaseRequest(name, 4));
        boolean result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(14, s.availablePermits());
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
