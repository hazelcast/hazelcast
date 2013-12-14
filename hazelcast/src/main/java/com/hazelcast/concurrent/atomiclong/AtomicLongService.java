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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.core.Partition;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicLongService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:atomicLongService";
    private NodeEngine nodeEngine;

    private Partition[] partitions;

    public AtomicLongService() {
    }

    public void restorePartition(int partitionId, Map<String, Long> migrationData) {
        Partition partition = partitions[partitionId];
        partition.restorePartition(migrationData);
    }

    private class Partition{
        private final Map<String,AtomicLongWrapper> numbers = new HashMap<String,AtomicLongWrapper>();

        AtomicLongWrapper get(String name){
            AtomicLongWrapper number = numbers.get(name);
            if(number == null){
                number = new AtomicLongWrapper();
                numbers.put(name,number);
            }

            return number;
        }

        public Map<String,Long> toBackupData(){
            if(numbers.isEmpty()){
                return null;
            }

            Map<String,Long> data = new HashMap<String,Long>(numbers.size());
            for(Map.Entry<String,AtomicLongWrapper> entry: numbers.entrySet()){
                data.put(entry.getKey(),entry.getValue().get());
            }

            return data;
        }

        public void clear() {
            numbers.clear();
        }

        public void restorePartition(Map<String, Long> migrationData) {
            for(Map.Entry<String,Long> entry: migrationData.entrySet()){
                get(entry.getKey()).set(entry.getValue());
            }
        }

        public void remove(String name) {
            numbers.remove(name);
        }
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getGroupProperties().PARTITION_COUNT.getInteger();
        partitions = new Partition[partitionCount];
        for (int k = 0; k < partitionCount; k++) {
            partitions[k] = new Partition();
        }
    }

    public AtomicLongWrapper getNumber(int partitionId, String name) {
        Partition partition =  partitions[partitionId];
        return partition.get(name);
    }

    // need for testing..
    public boolean containsAtomicLong(String name) {
        for(Partition partition: partitions){
            if(partition.numbers.containsKey(name)){
                return true;
            }
        }

        return false;
    }

    public void reset() {
        for(Partition partition: partitions){
            partition.clear();
        }
    }

    public void shutdown(boolean terminate) {
        reset();
    }

    public AtomicLongProxy createDistributedObject(String name) {
        return new AtomicLongProxy(name, nodeEngine, this);
    }

    public void destroyDistributedObject(String name) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
        Partition partition = partitions[partitionId];
        partition.remove(name);
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }
        Map<String, Long> data = partitions[event.getPartitionId()].toBackupData();
        if(data == null){
            return null;
        }

        return new AtomicLongReplicationOperation(data);
    }

    public void commitMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            removeNumber(partitionMigrationEvent.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeNumber(partitionMigrationEvent.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        removeNumber(partitionId);
    }

    public void removeNumber(int partitionId) {
        Partition partition = partitions[partitionId];
        partition.clear();
    }
}
