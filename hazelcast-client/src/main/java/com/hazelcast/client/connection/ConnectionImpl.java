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

package com.hazelcast.client.connection;

import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.ObjectDataInputStream;
import com.hazelcast.nio.serialization.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Holds the socket to one of the members of Hazelcast Cluster.
 *
 * @author fuad-malikov
 */
final class ConnectionImpl implements Connection {

    private static int CONN_ID = 1;
    private static final int BUFFER_SIZE = 16 << 10; // 32k

    private static synchronized int newConnId() {
        return CONN_ID++;
    }

    private final Socket socket;
    private final Address endpoint;
    private final ObjectDataOutputStream out;
    private final ObjectDataInputStream in;
    private final int id = newConnId();
    private volatile long lastRead = Clock.currentTimeMillis();

    public ConnectionImpl(Address address, SocketOptions options, SerializationService serializationService) throws IOException {
        this.endpoint = address;
        final InetSocketAddress isa = address.getInetSocketAddress();
        final Socket socket = new Socket();
        try {
            socket.setKeepAlive(options.isSocketKeepAlive());
            socket.setTcpNoDelay(options.isSocketTcpNoDelay());
            socket.setReuseAddress(options.isSocketReuseAddress());
            if (options.getSocketLingerSeconds() > 0) {
                socket.setSoLinger(true, options.getSocketLingerSeconds());
            }
            if (options.getSocketTimeout() > 0) {
                socket.setSoTimeout(options.getSocketTimeout());
            }
            int bufferSize = options.getSocketBufferSize() * 1024;
            if (bufferSize < 0) {
                bufferSize = BUFFER_SIZE;
            }
            socket.setSendBufferSize(bufferSize);
            socket.setReceiveBufferSize(bufferSize);
            socket.connect(isa, 3000);
        } catch (IOException e) {
            socket.close();
            throw e;
        }
        this.socket = socket;
        this.out = serializationService.createObjectDataOutputStream(
                new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE));
        this.in = serializationService.createObjectDataInputStream(
                new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
    }

    Socket getSocket() {
        return socket;
    }

    @Override
    public Address getEndpoint() {
        return endpoint;
    }

    void write(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    @Override
    public boolean write(Data data) throws IOException {
        data.writeData(out);
        out.flush();
        return true;
    }

    @Override
    public Data read() throws IOException {
        Data data = new Data();
        data.readData(in);
        lastRead = Clock.currentTimeMillis();
        return data;
    }

    @Override
    public void release() throws IOException {
        out.close();
        in.close();
        socket.close();
    }

    @Override
    public void close() throws IOException {
        release();
    }

    @Override
    public int getId() {
        return id;
    }

    public long getLastReadTime() {
        return lastRead;
    }

    @Override
    public String toString() {
        return "Connection [" + endpoint + " -> " +
                socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + "]";
    }
}
