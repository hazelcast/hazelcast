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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.WriteResult;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 5/14/13
 */
public class MockSimpleClient implements SimpleClient {

    private static final AtomicInteger port = new AtomicInteger(9000);

    private final ClientEngineImpl clientEngine;
    private final SerializationService serializationService;
    private final MockConnection connection;

    public MockSimpleClient(ClientEngineImpl clientEngine,
                            SerializationService serializationService) throws UnknownHostException {
        this.clientEngine = clientEngine;
        this.serializationService = serializationService;
        this.connection = new MockConnection(port.incrementAndGet());
    }

    public void auth() throws IOException {
        //we need to call this so that the endpoint is created for the connection. Normally this is done from
        //the ConnectionManager.
        clientEngine.getConnectionListener().connectionAdded(connection);
        AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
        auth.setOwnerConnection(true);
        send(auth);
        receive();
    }

    public void send(Object o) throws IOException {
        Data data = serializationService.toData(o);
        Packet packet = new Packet(data, serializationService.getPortableContext());
        packet.setConn(connection);
        clientEngine.handlePacket(packet);
    }

    public Object receive() throws IOException {
        Packet packet;
        try {
            packet = (Packet) connection.q.take();
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
        ClientResponse clientResponse = serializationService.toObject(packet.getData());
        return serializationService.toObject(clientResponse.getResponse());
    }

    public void close() {
        final ClientEndpointManager endpointManager = clientEngine.getEndpointManager();
        final ClientEndpoint endpoint = endpointManager.getEndpoint(connection);
        endpointManager.removeEndpoint(endpoint, true);
        connection.close();
    }

    class MockConnection implements Connection {

        volatile boolean live = true;

        final int port;

        MockConnection(int port) {
            this.port = port;
        }

        BlockingQueue<SocketWritable> q = new LinkedBlockingQueue<SocketWritable>();

        public WriteResult write(SocketWritable packet) {
            return q.offer(packet) ? WriteResult.SUCCESS : WriteResult.FAILURE;
        }

        @Override
        public Address getEndPoint() {
            return null;
        }

        @Override
        public boolean isAlive() {
            return live;
        }

        @Override
        public long lastReadTime() {
            return 0;
        }

        @Override
        public long lastWriteTime() {
            return 0;
        }

        @Override
        public void close() {
            live = false;
        }

        @Override
        public boolean isClient() {
            return true;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.BINARY_CLIENT;
        }

        @Override
        public InetAddress getInetAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return null;
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public void setAvailableSlots(Integer claimResponse) {

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MockConnection)) return false;

            MockConnection that = (MockConnection) o;

            if (port != that.port) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return port;
        }
    }
}
