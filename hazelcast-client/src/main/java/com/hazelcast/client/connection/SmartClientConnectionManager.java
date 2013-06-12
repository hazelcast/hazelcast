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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.Destructor;
import com.hazelcast.client.util.Factory;
import com.hazelcast.client.util.ObjectPool;
import com.hazelcast.client.util.QueueBasedObjectPool;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SmartClientConnectionManager implements ClientConnectionManager {

    private final int poolSize;
    private final Authenticator authenticator;
    private final HazelcastClient client;
    private final Router router;
    private final ConcurrentMap<Address, ObjectPool<ConnectionWrapper>> poolMap
            = new ConcurrentHashMap<Address, ObjectPool<ConnectionWrapper>>(16, 0.75f, 1);
    private final SocketOptions socketOptions;
    private final SocketInterceptor socketInterceptor;
    private final HeartBeatChecker heartbeat;

    private volatile boolean live = true;

    public SmartClientConnectionManager(HazelcastClient client, Authenticator authenticator, LoadBalancer loadBalancer) {
        this.authenticator = authenticator;
        this.client = client;
        ClientConfig config = client.getClientConfig();
        router = new Router(loadBalancer);
        socketInterceptor = config.getSocketInterceptor();
        poolSize = config.getPoolSize();
        int connectionTimeout = config.getConnectionTimeout();
        heartbeat = new HeartBeatChecker(connectionTimeout, client.getSerializationService(), client.getClientExecutionService());
        socketOptions = config.getSocketOptions();
    }

    public Connection firstConnection(Address address, Authenticator authenticator) throws IOException {
        return newConnection(address, authenticator);
    }

    public Connection newConnection(Address address, Authenticator authenticator) throws IOException {
        checkLive();
        final ConnectionImpl connection = new ConnectionImpl(address, socketOptions, client.getSerializationService());
        connection.write(Protocols.CLIENT_BINARY.getBytes());
        if (socketInterceptor != null) {
            socketInterceptor.onConnect(connection.getSocket());
        }
        authenticator.auth(connection);
        return connection;
    }

    public Connection getRandomConnection() throws IOException {
        checkLive();
        final Address address = router.next();
        if (address == null) {
            throw new IOException("LoadBalancer '" + router + "' could not find a address to route to");
        }
        return getConnection(address);
    }

    public Connection getConnection(Address address) throws IOException {
        checkLive();
        if (address == null) {
            throw new IllegalArgumentException("Target address is required!");
        }
        final ObjectPool<ConnectionWrapper> pool = getConnectionPool(address);
        Connection connection = null;
        try {
            connection = pool == null ? getRandomConnection() : pool.take();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Could be that this address is dead and that's why pool is not able to create and give a connection.
        // We will call it again, and hopefully at some time LoadBalancer will give us the right target for the connection.
        if (connection == null) {
            checkLive();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            return getRandomConnection();
        }
        if (!heartbeat.checkHeartBeat(connection)) {
            connection.close();
            return getRandomConnection();
        }
        return connection;
    }

    private void checkLive() {
        if (!live) {
            throw new HazelcastInstanceNotActiveException();
        }
    }

    private final ConstructorFunction<Address, ObjectPool<ConnectionWrapper>> ctor = new ConstructorFunction<Address, ObjectPool<ConnectionWrapper>>() {
        public ObjectPool<ConnectionWrapper> createNew(final Address address) {
            return new QueueBasedObjectPool<ConnectionWrapper>(poolSize, new Factory<ConnectionWrapper>() {
                public ConnectionWrapper create() throws IOException {
                    return new ConnectionWrapper(newConnection(address, authenticator));
                }
            }, new Destructor<ConnectionWrapper>() {
                public void destroy(ConnectionWrapper connection) {
                    connection.close();
                }
            }
            );
        }
    };

    private ObjectPool<ConnectionWrapper> getConnectionPool(final Address address) {
        checkLive();
        ObjectPool<ConnectionWrapper> pool = poolMap.get(address);
        if (pool == null) {
            if (client.getClientClusterService().getMember(address) == null){
                return null;
            }
            pool = ctor.createNew(address);
            ObjectPool<ConnectionWrapper> current = poolMap.putIfAbsent(address, pool);
            pool = current == null ? pool : current;
        }
        return pool;
    }

    private class ConnectionWrapper implements Connection {
        final Connection connection;

        private ConnectionWrapper(Connection connection) {
            this.connection = connection;
        }

        public Address getEndpoint() {
            return connection.getEndpoint();
        }

        public boolean write(Data data) throws IOException {
            return connection.write(data);
        }

        public Data read() throws IOException {
            return connection.read();
        }

        public void release() throws IOException {
            releaseConnection(this);
        }

        public void close() {
            IOUtil.closeResource(connection);
        }

        public int getId() {
            return connection.getId();
        }

        public long getLastReadTime() {
            return connection.getLastReadTime();
        }

        public void setEndpoint(Address address) {
            connection.setEndpoint(address);
        }
    }

    private void releaseConnection(ConnectionWrapper connection) {
        if (live) {
            final ObjectPool<ConnectionWrapper> pool = poolMap.get(connection.getEndpoint());
            if (pool != null) {
                pool.release(connection);
            } else {
                connection.close();
            }
        } else {
            connection.close();
        }
    }

    public void removeConnectionPool(Address address){
        ObjectPool<ConnectionWrapper> pool = poolMap.remove(address);
        if (pool != null){
            pool.destroy();
        }
    }

    public void shutdown() {
        live = false;
        for (ObjectPool<ConnectionWrapper> pool : poolMap.values()) {
            pool.destroy();
        }
        poolMap.clear();
    }
}
