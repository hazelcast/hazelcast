/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Member;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionPool {
    private final ConnectionManager connectionManager;

    static private final int POOL_SIZE = 1;
    Set<InetSocketAddress> addresses = new HashSet();
//    Map<InetSocketAddress, BlockingQueue<Connection>> mPool;
    BlockingQueue<Connection> allConnectionsQ = new LinkedBlockingQueue<Connection>(POOL_SIZE);

    public ConnectionPool(HazelcastClient client, ClientConfig config, ConnectionManager connectionManager) {
        this.addresses.addAll(config.getAddressList());
        for (InetSocketAddress address : addresses) {
            try {
                Connection initialConnection = new Connection(address, 0, null);
                connectionManager.bindConnection(initialConnection);
                allConnectionsQ.offer(initialConnection);
                System.out.println("Client " + client);
                Set<Member> members = client.getCluster().getMembers();
//                mPool = new ConcurrentHashMap<InetSocketAddress, BlockingQueue<Connection>>(members.size());
                for (Member member : members) {
                    InetSocketAddress isa = member.getInetSocketAddress();
                    addresses.add(isa);
//                    BlockingQueue<Connection> pool = new LinkedBlockingQueue<Connection>(POOL_SIZE);
//                    mPool.put(isa, pool);
                    if (address.equals(isa))
//                        pool.offer(initialConnection);
                    allConnectionsQ.offer(initialConnection);
//                    while (allConnectionsQ.size() < POOL_SIZE) {
//                        Connection connection = new Connection(isa, 0);
//                        connectionManager.bindConnection(connection);
//                        pool.offer(connection);
//                        allConnectionsQ.offer(connection);
//                    }
                }
                break;
            } catch (IOException e) {
                continue;
            }
        }
        this.connectionManager = connectionManager;
        while (allConnectionsQ.size() < POOL_SIZE) {
            try {
                Connection connection = connectionManager.createNextConnection();
                connectionManager.bindConnection(connection);
                allConnectionsQ.offer(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Connection takeConnection() throws InterruptedException {
        return allConnectionsQ.take();
    }

    public void releaseConnection(Connection connection) {
        allConnectionsQ.offer(connection);
    }
}
