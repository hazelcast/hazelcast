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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.concurrent.atomiclong.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/13/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class AtomicLongBinaryClientTest {

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
        hz.getAtomicLong(name).set(0);
    }

    @After
    public void closeClient() throws IOException {
        c.close();
        hz.getAtomicLong(name).set(0);
    }

    @Test
    public void testAddAndGet() throws Exception {
        c.send(new AddAndGetRequest(name, 3));
        long result = (Long) c.receive();
        assertEquals(3, result);

        c.send(new AddAndGetRequest(name, 4));
        result = (Long) c.receive();
        assertEquals(7, result);
    }

    @Test
    public void testCompareAndSet() throws Exception {
        IAtomicLong atomicLong = hz.getAtomicLong(name);
        atomicLong.set(11);

        c.send(new CompareAndSetRequest(name, 9, 5));
        boolean result = (Boolean) c.receive();
        assertFalse(result);
        assertEquals(11, atomicLong.get());

        c.send(new CompareAndSetRequest(name, 11, 5));
        result = (Boolean) c.receive();
        assertTrue(result);
        assertEquals(5, atomicLong.get());
    }

    @Test
    public void testGetAndAdd() throws IOException {
        IAtomicLong atomicLong = hz.getAtomicLong(name);
        atomicLong.set(11);

        c.send(new GetAndAddRequest(name, 4));
        long result = (Long) c.receive();
        assertEquals(11, result);
        assertEquals(15, atomicLong.get());


    }

    @Test
    public void testGetAndSet() throws IOException {
        IAtomicLong atomicLong = hz.getAtomicLong(name);
        atomicLong.set(11);

        c.send(new GetAndSetRequest(name, 9));
        long result = (Long) c.receive();
        assertEquals(11, result);
        assertEquals(9, atomicLong.get());


    }

    @Test
    public void testSet() throws IOException {
        IAtomicLong atomicLong = hz.getAtomicLong(name);
        atomicLong.set(11);

        c.send(new SetRequest(name, 7));
        c.receive();
        assertEquals(7, atomicLong.get());


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
