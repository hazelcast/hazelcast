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
import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.core.PartitionService;

import java.net.UnknownHostException;
import java.util.LinkedHashSet;
import java.util.Set;


public class PartitionClientProxy implements PartitionService {
    final private ProxyHelper proxyHelper;

    public PartitionClientProxy(HazelcastClient client) {
        proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
    }

    public Set<Partition> getPartitions() {
        Protocol protocol = proxyHelper.doCommand(Command.PARTITIONS, new String[]{}, null);
        Set<Partition> set = new LinkedHashSet<Partition>();
        int i=protocol.args.length;

        while(i>0){
            final int partitionId = Integer.valueOf(protocol.args[i]);
            String owner = protocol.args[++i];
            Partition partition = partition(partitionId, owner);
            set.add(partition);
        }
        return set;
    }

    private Partition partition(final int partitionId, String owner) {
        Address address = null;
        if(owner!="null"){
            String[] a = owner.split(":");
            try {
                address = new Address(a[0], Integer.valueOf(a[1]));
            } catch (UnknownHostException e) {
            }
        }
        final Member member = address == null? null: new MemberImpl(address, false);
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
        return partition(Integer.valueOf(protocol.args[0]), protocol.args[1]);
    }

    public void addMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }

    public void removeMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }
}
