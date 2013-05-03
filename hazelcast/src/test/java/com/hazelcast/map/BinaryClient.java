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

import com.hazelcast.clientv2.ClientAuthenticationRequest;
import com.hazelcast.clientv2.ClientRequest;
import com.hazelcast.map.clientv2.MapGetRequest;
import com.hazelcast.map.clientv2.MapPutRequest;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.GroupProperties;
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
        Hazelcast.newHazelcastInstance(config);

        Socket socket = new Socket();
        SerializationService service = new SerializationServiceImpl(0);
        socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5701));
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(new byte[]{'C', 'B', '1'});
        outputStream.flush();
        final ObjectDataInputStream in = service.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
        final ObjectDataOutputStream out = service.createObjectDataOutputStream(new BufferedOutputStream(outputStream));

        ClientAuthenticationRequest auth = new ClientAuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
        invoke(service, in, out, auth);

        MapPutRequest op = new MapPutRequest("test", service.toData(1), service.toData(1));
        invoke(service, in, out, op);

        MapGetRequest get = new MapGetRequest("test", service.toData(1));
        invoke(service, in, out, get);

        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread.sleep(2000);
        Hazelcast.shutdownAll();

    }

    private static void invoke(SerializationService service, ObjectDataInputStream in, ObjectDataOutputStream out, ClientRequest op) throws IOException {
        final Data data = service.toData(op);
        data.writeData(out);
        out.flush();

        Data responseData = new Data();
        responseData.readData(in);
        final Object result = service.toObject(responseData);
        System.err.println("result = " + result);
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
