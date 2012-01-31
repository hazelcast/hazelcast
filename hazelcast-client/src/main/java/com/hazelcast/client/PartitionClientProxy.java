/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.Keys;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.util.LinkedHashSet;
import java.util.Set;

public class PartitionClientProxy implements PartitionService {
    final private ProxyHelper proxyHelper;

    public PartitionClientProxy(HazelcastClient client) {
        proxyHelper = new ProxyHelper("", client);
    }

    public Set<Partition> getPartitions() {
        Keys partitions =
                (Keys) proxyHelper.doOp(ClusterOperation.CLIENT_GET_PARTITIONS, null, null);
        Set<Partition> set = new LinkedHashSet<Partition>();
        for (Data d : partitions.getKeys()) {
            set.add((Partition) IOUtil.toObject(d.buffer));
        }
        return set;
    }

    public Partition getPartition(Object key) {
        return (Partition) proxyHelper.doOp(ClusterOperation.CLIENT_GET_PARTITIONS, key, null);
    }

    public void addMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }

    public void removeMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }
}
