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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.pool.ObjectPool;
import com.hazelcast.client.util.pool.QueueBasedObjectPool;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.Partition;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {
    static private final int POOL_SIZE = 2;

    private final ConnectionManager connectionManager;
    private final SerializationService serializationService;

    public final ConcurrentHashMap<Integer, Member> partitionTable = new ConcurrentHashMap<Integer, Member>(271);
    public final AtomicInteger partitionCount = new AtomicInteger(0);
    private final Router router;
    private final ConcurrentMap<Address, ObjectPool<Connection>> mPool = new ConcurrentHashMap<Address, ObjectPool<Connection>>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Connection initialConnection;

    public ConnectionPool(ClientConfig config, final ConnectionManager connectionManager, final SerializationService serializationService) {
        this.connectionManager = connectionManager;
        this.serializationService = serializationService;
        initialConnection = initialConnection(config);
        router = config.getRouter();
    }

    public void init(HazelcastInstance hazelcast, final PartitionService partitionService) {
        router.init(hazelcast);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                createPartitionTable(partitionService);
            }
        }, 0, 1000);
        partitionCount.set(partitionTable.size());
        initialized.set(true);
    }

    private void createPartitionTable(PartitionService partitionService) {
        Set<Partition> partitions = partitionService.getPartitions();
        for (Partition p : partitions) {
            if (p.getOwner() != null)
                partitionTable.put(p.getPartitionId(), p.getOwner());
        }
    }

    private Connection initialConnection(ClientConfig config) {
        Connection initialConnection;
        for (InetSocketAddress isa : config.getAddressList()) {
            try {
                Address address = new Address(isa);
                initialConnection = new Connection(address, 0, this.serializationService);
                this.connectionManager.bindConnection(initialConnection);
                return initialConnection;
            } catch (IOException e) {
                continue;
            }
        }
        throw new RuntimeException("Couldn't connect to any address in the config");
    }

    private ObjectPool<Connection> createPoolForTheMember(MemberImpl member) {
        final Address address = member.getAddress();
        ObjectPool<Connection> pool = new QueueBasedObjectPool<Connection>(POOL_SIZE, new com.hazelcast.client.util.pool.Factory<Connection>() {
            @Override
            public Connection create() throws IOException {
                Connection connection = new Connection(address, 0, serializationService);
                connectionManager.bindConnection(connection);
                return connection;
            }
        });
        if (mPool.putIfAbsent(address, pool) != null) {
            return mPool.get(address);
        }
        if (address.equals(initialConnection.getAddress()))
            pool.add(initialConnection);
        return pool;
    }

    public Connection takeConnection(Member member) throws InterruptedException {
        if (!initialized.get())
            return initialConnection;
        if (member == null) {
            member = router.next();
            if (member == null) {
                throw new RuntimeException("Router '"+router.toString()+"' has no members to route to");
            }
        }
        ObjectPool<Connection> pool = mPool.get(member.getInetSocketAddress());
        if (pool == null) {
            pool = createPoolForTheMember((MemberImpl) member);
        }
        return pool.take();
    }

    public void releaseConnection(Connection connection) {
        ObjectPool<Connection> pool = mPool.get(connection.getAddress());
        if (pool != null)
            pool.release(connection);
    }
}
