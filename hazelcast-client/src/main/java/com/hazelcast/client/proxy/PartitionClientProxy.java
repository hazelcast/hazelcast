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
package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.proxy.listener.MigrationEventLRH;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionClientProxy implements PartitionService {
    final private ProxyHelper proxyHelper;
    final private HazelcastClient client;
    Map<MigrationListener, ListenerThread> listenerMap = new ConcurrentHashMap<MigrationListener, ListenerThread>();
    private final ConcurrentHashMap<Integer, Partition> cachedParttitionTable = new ConcurrentHashMap<Integer, Partition>(271);
    private final AtomicInteger partitionCount = new AtomicInteger(0);

    public PartitionClientProxy(HazelcastClient client) {
        proxyHelper = new ProxyHelper(client);
        this.client = client;
    }

    public void initInternalPartitionTable() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    createPartitionTable(PartitionClientProxy.this);
                    partitionCount.set(cachedParttitionTable.size());
                } catch (Exception e) {
                }
            }
        }, 0, 1000);
    }

    public Partition getCachedPartition(Object object) {
        if (object == null) return null;
        if (partitionCount.get() == 0) {
            return null;
        }
        Data key = proxyHelper.toData(object);
        int id = Math.abs(key.getPartitionHash()) % partitionCount.get();
        Partition partition = cachedParttitionTable.get(id);
        return partition;
    }

    private void createPartitionTable(PartitionService partitionService) {
        Set<Partition> partitions = partitionService.getPartitions();
        for (Partition p : partitions) {
            if (p.getOwner() != null)
                cachedParttitionTable.put(p.getPartitionId(), p);
        }
    }

    public Set<Partition> getPartitions() {
        Protocol protocol = proxyHelper.doCommand(Command.PARTITIONS, new String[]{});
        Set<Partition> set = new LinkedHashSet<Partition>();
        int i = 0;
        while (i < protocol.args.length - 1) {
            final int partitionId = Integer.valueOf(protocol.args[i++]);
            String hostname = protocol.args[i++];
            int port = Integer.valueOf(protocol.args[i++]);
            Partition partition = partition(partitionId, hostname, port);
            set.add(partition);
        }
        return set;
    }

    private Partition partition(final int partitionId, String hostname, int port) {
        Address address = null;
        if (hostname != null && !hostname.equals("null")) {
            try {
                address = new Address(hostname, port);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        final Member member = address == null ? null : new MemberImpl(address, false);
        return new Partition() {
            public int getPartitionId() {
                return partitionId;
            }

            public Member getOwner() {
                return member;
            }
        };
    }

    public Partition getPartition(Object key) {
        Protocol protocol = proxyHelper.doCommand(Command.PARTITIONS, new String[]{}, proxyHelper.toData(key));
        return partition(Integer.valueOf(protocol.args[0]), protocol.args[1], Integer.valueOf(protocol.args[2]));
    }

    public void addMigrationListener(MigrationListener migrationListener) {
        Protocol request = proxyHelper.createProtocol(Command.MIGRATIONLISTEN, null, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.migrationListener.",
                client, request, new MigrationEventLRH(migrationListener, this));
        listenerMap.put(migrationListener, thread);
        thread.start();
    }

    public void removeMigrationListener(MigrationListener migrationListener) {
        ListenerThread thread = listenerMap.remove(migrationListener);
        if (thread != null) {
            thread.stopListening();
        }
    }

    public boolean hasOngoingMigration() {
        throw new UnsupportedOperationException("Client doesn't support this operation");
    }
}
