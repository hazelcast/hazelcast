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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.pool.Factory;
import com.hazelcast.client.util.pool.ObjectPool;
import com.hazelcast.client.util.pool.QueueBasedObjectPool;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionManager {
    private final int poolSize;
    private final int connectionTimeout;
    private final Authenticator authenticator;
    private final SerializationService serializationService;
    private final Router router;
    private final ConcurrentMap<Address, ObjectPool<Connection>> poolMap = new ConcurrentHashMap<Address, ObjectPool<Connection>>();
    private final SocketInterceptor socketInterceptor;
    private final HeartBeatChecker heartbeat;

    private volatile boolean live = true;

    public ConnectionManager(ClientConfig config, Authenticator authenticator, SerializationService serializationService) {
        this.authenticator = authenticator;
        this.serializationService = serializationService;
        router = new Router(config.getLoadBalancer());
        socketInterceptor = config.getSocketInterceptor();
        poolSize = config.getPoolSize();
        connectionTimeout = config.getConnectionTimeout();
        heartbeat = new HeartBeatChecker(config, serializationService);
    }

    public Connection newConnection(Address address) throws IOException {
        return newConnection(address, authenticator);
    }

    public Connection newConnection(Address address, Authenticator authenticator) throws IOException {
        checkLive();
        final ConnectionImpl connection = new ConnectionImpl(address, serializationService);
        connection.write(Protocols.CLIENT_BINARY.getBytes());
        if (socketInterceptor != null) {
            socketInterceptor.onConnect(connection.getSocket());
        }
        authenticator.auth(connection);
        return connection;
    }

    public Connection getConnection(Address address) throws IOException {
        checkLive();
        if (address == null) {
            address = router.next();
            if (address == null) {
                throw new IOException("LoadBalancer '" + router + "' could not find a address to route to");
            }
        }
        final ObjectPool<Connection> pool = getConnectionPool(address);
        final Connection connection = pool.take();
        // Could be that this address is dead and that's why pool is not able to create and give a connection.
        // We will call it again, and hopefully at some time LoadBalancer will give us the right target for the connection.
        if (connection == null) {
            checkLive();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
            return getConnection(null);
        }
        if (!heartbeat.checkHeartBeat(connection)) {
            try {
                connection.close();
            } catch (IOException ignored) {
            }
            return getConnection(null);
        }
        return connection;
    }

    private void checkLive() {
        if (!live) {
            throw new HazelcastInstanceNotActiveException();
        }
    }

    private final ConstructorFunction<Address, ObjectPool<Connection>> ctor = new ConstructorFunction<Address, ObjectPool<Connection>>() {
        public ObjectPool<Connection> createNew(final Address address) {
            return new QueueBasedObjectPool<Connection>(poolSize, new Factory<Connection>() {
                public Connection create() throws IOException {
                    return new ConnectionWrapper(newConnection(address));
                }
            });
        }
    };

    private ObjectPool<Connection> getConnectionPool(final Address address) {
        checkLive();
        return ConcurrencyUtil.getOrPutIfAbsent(poolMap, address, ctor);
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

        public void close() throws IOException {
            releaseConnection(this);
        }

        public int getId() {
            return connection.getId();
        }
    }

    private void releaseConnection(Connection connection) {
        ObjectPool<Connection> pool = poolMap.get(connection.getEndpoint());
        if (pool != null)
            pool.release(connection);
    }

    public void shutdown() {
        live = false;
        for (ObjectPool<Connection> pool : poolMap.values()) {
            pool.destroy();
        }
        poolMap.clear();
    }
}
