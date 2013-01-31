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
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {
    static private final int POOL_SIZE = 2;
    private Map<Address, BlockingQueue<Connection>> mPool;
    //    private Connection initialConnection;
    private BlockingQueue<Connection> singlePool = new LinkedBlockingQueue<Connection>(POOL_SIZE);
    private final ConnectionManager connectionManager;
    private final SerializationService serializationService;
    private volatile Map<Integer, Member> partitionTable = new ConcurrentHashMap<Integer, Member>(271);
    private final AtomicInteger partitionCount = new AtomicInteger(0);

    public ConnectionPool(ClientConfig config, final ConnectionManager connectionManager, final SerializationService serializationService) {
        this.connectionManager = connectionManager;
        this.serializationService = serializationService;
        initialConnection(config);
    }

    public void init(Cluster cluster, final PartitionService partitionService) {
        addMembershipListener(cluster);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                createPartitionTable(partitionService);
            }
        }, 0, 1000);
        partitionCount.set(partitionTable.size());
        Set<Member> members = cluster.getMembers();
        mPool = new ConcurrentHashMap<Address, BlockingQueue<Connection>>(members.size());
        for (Member _member : members) {
            createPoolForTheMember((MemberImpl) _member);
        }
    }

    private void createPartitionTable(PartitionService partitionService) {
        Set<Partition> partitions = partitionService.getPartitions();
        for (Partition p : partitions) {
            System.out.println("Partition element" + p.getPartitionId() + ": " + p.getOwner());
            if (p.getOwner() != null)
                partitionTable.put(p.getPartitionId(), p.getOwner());
        }
    }


    private void addMembershipListener(final Cluster cluster) {
        cluster.addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                createPoolForTheMember((MemberImpl) membershipEvent.getMember());
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                MemberImpl member = (MemberImpl) membershipEvent.getMember();
                mPool.remove(member.getAddress());
            }
        });
    }

    private void initialConnection(ClientConfig config) {
        Connection initialConnection = null;
        for (InetSocketAddress isa : config.getAddressList()) {
            try {
                int i = 0;
                while (i++ < POOL_SIZE) {
                    initialConnection = new Connection(new Address(isa), 0, this.serializationService);
                    System.out.println("Intial connection " + initialConnection);
                    this.connectionManager.bindConnection(initialConnection);
                    singlePool.offer(initialConnection);
                }
                break;
            } catch (IOException e) {
                continue;
            }
        }
        if (initialConnection == null) {
            throw new RuntimeException("Couldn't connect to any address in the config");
        }
    }

    private void createPoolForTheMember(MemberImpl member) {
        try {
            Address address = member.getAddress();
            BlockingQueue<Connection> pool = new LinkedBlockingQueue<Connection>(POOL_SIZE);
            mPool.put(address, pool);
            while (pool.size() < POOL_SIZE) {
                Connection connection = new Connection(address, 0, serializationService);
                connectionManager.bindConnection(connection);
                pool.offer(connection);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection takeConnection() throws InterruptedException {
        System.out.println(singlePool.size() + " : taking connection" + Thread.currentThread().getName());
        return singlePool.take();
    }

    public Connection takeConnection(Data key) throws InterruptedException, UnknownHostException {
        if (key == null) return takeConnection();
        if (partitionCount.get() == 0) {
            return mPool.values().iterator().next().take();
        }
        int id = key.getPartitionHash() % partitionCount.get();
        Member member = partitionTable.get(id);
        if (member == null) {
            return singlePool.take();
        }
        return mPool.get(member.getInetSocketAddress()).take();
    }

    public void releaseConnection(Connection connection) {
        System.out.println(singlePool.size() + " : release connection" + Thread.currentThread().getName());
        singlePool.offer(connection);
    }

    public void releaseConnection(Connection connection, Data key) {
        if (key == null) {
            releaseConnection(connection);
            return;
        }
        mPool.get(connection.getAddress()).offer(connection);
    }
}
