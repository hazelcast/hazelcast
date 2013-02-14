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
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.Partition;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {
    static private final int POOL_SIZE = 2;
    private Map<Address, ObjectPool<Connection>> mPool;
    private BlockingQueue<Connection> singlePool = new LinkedBlockingQueue<Connection>(POOL_SIZE);
    private final ConnectionManager connectionManager;
    private final SerializationService serializationService;
    public volatile Map<Integer, Member> partitionTable = new ConcurrentHashMap<Integer, Member>(271);
    public final AtomicInteger partitionCount = new AtomicInteger(0);
    public volatile Router router;

    public ConnectionPool(ClientConfig config, final ConnectionManager connectionManager, final SerializationService serializationService) {
        this.connectionManager = connectionManager;
        this.serializationService = serializationService;
        initialConnection(config);

    }

    public void init(Cluster cluster, final PartitionService partitionService) {
        router = new RandomRouter(cluster);
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
        mPool = new ConcurrentHashMap<Address, ObjectPool<Connection>>(members.size());
        for (Member _member : members) {
            createPoolForTheMember((MemberImpl) _member);
        }
    }

    private void createPartitionTable(PartitionService partitionService) {
        Set<Partition> partitions = partitionService.getPartitions();
        for (Partition p : partitions) {
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
            ObjectPool<Connection> pool = new QueueBasedObjectPool<Connection>(POOL_SIZE);
            mPool.put(address, pool);
            while (pool.size() < POOL_SIZE) {
                Connection connection = new Connection(address, 0, serializationService);
                connectionManager.bindConnection(connection);
                pool.add(connection);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection takeConnection(Member member) throws InterruptedException {
        if (member == null) {
            member = router.next();
        }
        return mPool.get(member.getInetSocketAddress()).take();
    }

    public void releaseConnection(Connection connection, Member member) {
        if (member == null) {
            singlePool.offer(connection);
            return;
        }
        mPool.get(connection.getAddress()).release(connection);
    }

    interface Router {
        public Member next();
    }

    class StaticRouter implements Router {
        final Member member;

        StaticRouter(Member member) {
            this.member = member;
        }

        @Override
        public Member next() {
            return member;
        }
    }

    class RandomRouter implements Router, MembershipListener {
        List<Member> members = new CopyOnWriteArrayList<Member>();
        AtomicInteger index = new AtomicInteger(0);
        Random random = new Random(System.currentTimeMillis());

        RandomRouter(Cluster cluster) {
            cluster.addMembershipListener(this);
            members.addAll(cluster.getMembers());
        }

        @Override
        public Member next() {
            int i = random.nextInt(members.size());
            return members.get(i);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            members.add(membershipEvent.getMember());
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            members.remove(membershipEvent.getMember());
        }
    }
}
