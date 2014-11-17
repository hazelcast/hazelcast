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

package com.hazelcast.client;

import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author ali 5/14/13
 */
public class SocketSimpleClient implements SimpleClient {

    final Socket socket = new Socket();
    final ObjectDataInputStream in;
    final ObjectDataOutputStream out;
    final SerializationService serializationService;

    public SocketSimpleClient(Node node) throws IOException {
        socket.connect(node.address.getInetSocketAddress());
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(Protocols.CLIENT_BINARY.getBytes());
        outputStream.write(ClientTypes.JAVA.getBytes());
        outputStream.flush();
        serializationService = node.getSerializationService();
        in = serializationService.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = serializationService.createObjectDataOutputStream(new BufferedOutputStream(outputStream));
    }

    public void auth() throws IOException {
        AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
        send(auth);
        receive();
    }

    public void send(Object o) throws IOException {
        Data data = serializationService.toData(o);
        out.writeData(data);
        out.flush();
    }

    public Object receive() throws IOException {
        Data responseData = in.readData();
        ClientResponse clientResponse = serializationService.toObject(responseData);
        return serializationService.toObject(clientResponse.getResponse());
    }

    public void close() throws IOException {
        socket.close();
    }

}
