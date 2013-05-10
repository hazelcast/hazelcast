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
import com.hazelcast.map.client.MapAddEntryListenerRequest;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class BinaryClient {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> map = hz.getMap("test");

        map.put(1,1);

        final SerializationService ss = new SerializationServiceImpl(0);

        final Client c = new Client();
        c.auth();
        final Thread thread = new Thread() {
            public void run() {
                try {
                    c.send(new MapAddEntryListenerRequest("test"));
//                    c.send(new MapPutRequest("test", TestUtil.toData(1), TestUtil.toData(1), 778 ));
                    while (!isInterrupted()) {
                        System.err.println("--> " + c.receive());
                    }
                    c.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
        Thread.sleep(100);


        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }
        Thread.sleep(10);
        thread.interrupt();
        Thread.sleep(1000);
        c.close();
        thread.join();

        map.put(1111, 1111);

//        try {
//            c.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Thread.sleep(2000);
        Hazelcast.shutdownAll();

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
