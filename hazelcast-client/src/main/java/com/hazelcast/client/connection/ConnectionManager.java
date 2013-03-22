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

import com.hazelcast.client.Router;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.pool.ObjectPool;
import com.hazelcast.client.util.pool.QueueBasedObjectPool;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionManager {
    private final int poolSize;
    private final int connectionTimeout;
    private final SerializationService serializationService;
    private final DefaultClientBinder binder;
    private final Router router;
    private final ConcurrentMap<Address, ObjectPool<Connection>> mPool = new ConcurrentHashMap<Address, ObjectPool<Connection>>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Connection initialConnection;
    private final SocketInterceptor socketInterceptor;
    private final Lock lock = new ReentrantLock();
    private HazelcastInstance hazelcastInstance;
    private final HeartBeatChecker heartbeat;
//    final AtomicLong waitCount = new AtomicLong(0l);

    public ConnectionManager(ClientConfig config, final SerializationService serializationService) {
        this.serializationService = serializationService;
        binder = new DefaultClientBinder(serializationService, config.getCredentials());
        initialConnection = initialConnection(config);
        router = config.getRouter();
        socketInterceptor = config.getSocketInterceptor();
        poolSize = config.getPoolSize();
        connectionTimeout = config.getConnectionTimeout();
        heartbeat = new HeartBeatChecker(config, serializationService);
    }

    public void init(HazelcastInstance hazelcast) {
        router.init(hazelcast);
        this.hazelcastInstance = hazelcastInstance;
        initialized.set(true);
    }

    private Connection initialConnection(ClientConfig config) {
        final long next = Clock.currentTimeMillis() + config.getReConnectionTimeOut();
        int attempt = 0;
        Connection initialConnection;
        while()
        for (InetSocketAddress isa : config.getAddressList()) {
            try {
                Address address = new Address(isa);
                initialConnection = newConnection(address);
                return initialConnection;
            } catch (IOException e) {
                continue;
            }
        }
        if (attempt >= config.getInitialConnectionAttemptLimit()) {
            break;
        }

        throw new IllegalStateException("Unable to connect to any address in the config");
    }

    public Connection newConnection(Address address) throws IOException {
        Connection connection = new Connection(address, 0, serializationService);
        if (socketInterceptor != null)
            socketInterceptor.onConnect(connection.getSocket());
        binder.bind(connection);
        return connection;
    }

    public Connection takeConnection(Member member) throws InterruptedException {
        if (!initialized.get()) {
            lock.lock();
            return initialConnection;
        }
        if (member == null) {
            member = router.next();
            if (member == null) {
                throw new RuntimeException("Router '" + router + "' could not find a member to route to");
            }
        }
        ObjectPool<Connection> pool = mPool.get(member.getInetSocketAddress());
        if (pool == null) {
            synchronized (mPool) {
                pool = mPool.get(member.getInetSocketAddress());
                if (pool == null)
                    pool = createPoolForTheMember((MemberImpl) member);
            }
        }
        Connection connection = pool.take();
        //Could be that this member is dead and that's why pool is not able to create and give a connection.
        //We will call it again, and hopefully at some time Router will give us the right target for the connection.
        if (connection == null) {
            Thread.sleep(1000);
            return takeConnection(null);
        }
        if (!heartbeat.checkHeartBeat(connection)) {
            try {
                connection.close();
            } catch (IOException e) {
            }
            return takeConnection(null);
        }
        return connection;
    }

    private ObjectPool<Connection> createPoolForTheMember(MemberImpl member) {
        final Address address = member.getAddress();
        ObjectPool<Connection> pool = new QueueBasedObjectPool<Connection>(poolSize, new com.hazelcast.client.util.pool.Factory<Connection>() {
            @Override
            public Connection create() throws IOException {
                return newConnection(address);
            }
        });
        if (mPool.putIfAbsent(address, pool) != null) {
            return mPool.get(address);
        }
        if (address.equals(initialConnection.getAddress()))
            pool.add(initialConnection);
        return pool;
    }

    public void releaseConnection(Connection connection) {
        if (!initialized.get() && connection == initialConnection) {
            lock.unlock();
        }
        ObjectPool<Connection> pool = mPool.get(connection.getAddress());
        if (pool != null)
            pool.release(connection);
    }

    public Router getRouter() {
        return router;
    }
}
