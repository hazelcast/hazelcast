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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.ObjectDataInputStream;
import com.hazelcast.nio.serialization.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @ali 5/14/13
 */
public class SocketSimpleClient implements SimpleClient {

    private final Node node;
    final Socket socket = new Socket();
    final ObjectDataInputStream in;
    final ObjectDataOutputStream out;

    public SocketSimpleClient(Node node) throws IOException {
        this.node = node;
        socket.connect(node.address.getInetSocketAddress());
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(Protocols.CLIENT_BINARY.getBytes());
        outputStream.flush();
        SerializationService ss = getSerializationService();
        in = ss.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = ss.createObjectDataOutputStream(new BufferedOutputStream(outputStream));
    }

    public void auth() throws IOException {
        AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
        send(auth);
        receive();
    }

    public void send(Object o) throws IOException {
        final Data data = getSerializationService().toData(o);
        data.writeData(out);
        out.flush();
    }

    public Object receive() throws IOException {
        Data responseData = new Data();
        responseData.readData(in);
        return getSerializationService().toObject(responseData);
    }

    public void close() throws IOException {
        socket.close();
    }

    @Override
    public SerializationService getSerializationService() {
        return node.getSerializationService();
    }
}
