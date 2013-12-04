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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 1/10/13
 */
public class CountDownLatchService implements ManagedService, RemoteService, MigrationAwareService {

    public final static String SERVICE_NAME = "hz:impl:countDownLatchService";

    private final ConcurrentMap<String, CountDownLatchInfo> latches = new ConcurrentHashMap<String, CountDownLatchInfo>();
    private NodeEngine nodeEngine;

    public int getCount(String name) {
        final CountDownLatchInfo latch = latches.get(name);
        return latch != null ? latch.getCount() : 0;
    }

    public boolean setCount(String name, int count) {
        if (count <= 0) {
            latches.remove(name);
            return false;
        } else {
            CountDownLatchInfo latch = latches.get(name);
            if (latch == null) {
                latch = new CountDownLatchInfo(name);
                latches.put(name, latch);
            }
            return latch.setCount(count);
        }
    }

    public void setCountDirect(String name, int count) {
        if (count <= 0) {
            latches.remove(name);
        } else {
            CountDownLatchInfo latch = latches.get(name);
            if (latch == null) {
                latch = new CountDownLatchInfo(name);
                latches.put(name, latch);
            }
            latch.setCountDirect(count);
        }
    }

    public void countDown(String name) {
        final CountDownLatchInfo latch = latches.get(name);
        if (latch != null) {
            if (latch.countDown() == 0) {
                latches.remove(name);
            }
        }
    }

    public boolean shouldWait(String name) {
        final CountDownLatchInfo latch = latches.get(name);
        return latch != null && latch.getCount() > 0;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void reset() {
        latches.clear();
    }

    public void shutdown(boolean terminate) {
        latches.clear();
    }

    public CountDownLatchProxy createDistributedObject(String name) {
        return new CountDownLatchProxy(name, nodeEngine);
    }

    public void destroyDistributedObject(String name) {
        latches.remove(name);
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }
        final Collection<CountDownLatchInfo> data = new LinkedList<CountDownLatchInfo>();
        for (Map.Entry<String, CountDownLatchInfo> latchEntry : latches.entrySet()) {
            final String name = latchEntry.getKey();
            if (nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name)) == event.getPartitionId()) {
                data.add(latchEntry.getValue());
            }
        }
        return data.isEmpty() ? null : new CountDownLatchReplicationOperation(data);
    }

    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartition(event.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartition(event.getPartitionId());
        }
    }

    private void clearPartition(int partitionId) {
        final Iterator<String> iter = latches.keySet().iterator();
        while (iter.hasNext()) {
            final String name = iter.next();
            if (nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name)) == partitionId) {
                iter.remove();
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearPartition(partitionId);
    }

    public CountDownLatchInfo getLatch(String name) {
        return latches.get(name);
    }

    // need for testing..
    public boolean containsLatch(String name) {
        return latches.containsKey(name);
    }

    public void add(CountDownLatchInfo latch) {
        latches.put(latch.getName(), latch);
    }
}
