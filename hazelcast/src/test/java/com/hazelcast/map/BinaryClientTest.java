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
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.client.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class BinaryClientTest {

    @Test
    public void basicTest() throws Exception {
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        String mapName = "test";
        final IMap map = instance.getMap(mapName);

        Socket socket = new Socket();
        SerializationService service = new SerializationServiceImpl(0);
        socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5701));
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(new byte[]{'C', 'B', '1'});
        outputStream.flush();
        final ObjectDataInputStream in = service.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
        final ObjectDataOutputStream out = service.createObjectDataOutputStream(new BufferedOutputStream(outputStream));

        AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
        invoke(service, in, out, auth);

        int threadId = ThreadContext.getThreadId();
        MapPutRequest put = new MapPutRequest(mapName, service.toData(1), service.toData(1), threadId, -1);
        assertNull(invoke(service, in, out, put));
        map.put(1,2);
        put = new MapPutRequest(mapName, service.toData(1), service.toData(1), threadId, -1);
        assertEquals(2, invoke(service, in, out, put));

        // 1,1
        MapGetRequest get = new MapGetRequest(mapName, service.toData(1));
        assertEquals(1, invoke(service, in, out, get));

        MapPutIfAbsentRequest putIfAbsent = new MapPutIfAbsentRequest(mapName, service.toData(1), service.toData(3), threadId, -1);
        assertEquals(1, invoke(service, in, out, putIfAbsent));
        assertEquals(1, map.get(1));

        putIfAbsent = new MapPutIfAbsentRequest(mapName, service.toData(2), service.toData(2), threadId, -1);
        assertEquals(null, invoke(service, in, out, putIfAbsent));
        assertEquals(2, map.get(2));

        // 1,3  - 2,2

        new Thread(new Runnable() {
            @Override
            public void run() {
                map.lock(1);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
                map.unlock(1);
            }
        }).start();
        Thread.sleep(100);

        MapTryPutRequest tp = new MapTryPutRequest(mapName, service.toData(1), service.toData(1), threadId, 100);
        assertEquals(false, invoke(service, in, out, tp));

        MapTryPutRequest tp2 = new MapTryPutRequest(mapName, service.toData(1), service.toData(1), threadId, 2500);
        assertEquals(true, invoke(service, in, out, tp2));

        // 1,1 - 2,2
        assertEquals(1, map.get(1));

        MapPutTransientRequest treq = new MapPutTransientRequest(mapName, service.toData(1), service.toData(2), threadId, -1);
        invoke(service, in, out, treq);
        assertEquals(2, map.get(1));
        MapPutTransientRequest treq2 = new MapPutTransientRequest(mapName, service.toData(1), service.toData(1), threadId, -1);
        invoke(service, in, out, treq2);
        assertEquals(1, map.get(1));
        MapPutTransientRequest treq3 = new MapPutTransientRequest(mapName, service.toData(3), service.toData(3), threadId, -1);
        invoke(service, in, out, treq3);
        assertEquals(3, map.get(3));

        // 1,1 - 2,2
        assertEquals(1, map.get(1));
        assertEquals(2, map.get(2));
        assertEquals(3, map.get(3));

        MapSetRequest sreq = new MapSetRequest(mapName, service.toData(1), service.toData(2), threadId, -1);
        invoke(service, in, out, sreq);
        assertEquals(2, map.get(1));
        MapSetRequest sreq2 = new MapSetRequest(mapName, service.toData(1), service.toData(1), threadId, -1);
        invoke(service, in, out, sreq2);
        assertEquals(1, map.get(1));



        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread.sleep(2000);
        Hazelcast.shutdownAll();

    }

    private static Object invoke(SerializationService service, ObjectDataInputStream in, ObjectDataOutputStream out, Portable op) throws IOException {
        final Data data = service.toData(op);
        data.writeData(out);
        out.flush();

        Data responseData = new Data();
        responseData.readData(in);
        final Object result = service.toObject(responseData);
        return result;
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
