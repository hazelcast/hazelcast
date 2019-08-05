/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch;

import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.operations.CountDownLatchReplicationOperation;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.partition.MigrationAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.QuorumAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class CountDownLatchService implements ManagedService, RemoteService, MigrationAwareService, QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:countDownLatchService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, CountDownLatchContainer> containers
            = new ConcurrentHashMap<String, CountDownLatchContainer>();
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            CountDownLatchConfig countDownLatchConfig = nodeEngine.getConfig().findCountDownLatchConfig(name);
            String quorumName = countDownLatchConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    public int getCount(String name) {
        CountDownLatchContainer latch = containers.get(name);
        return latch != null ? latch.getCount() : 0;
    }

    public boolean setCount(String name, int count) {
        if (count < 0) {
            containers.remove(name);
            return false;
        } else {
            CountDownLatchContainer latch = containers.get(name);
            if (latch == null) {
                latch = new CountDownLatchContainer(name);
                containers.put(name, latch);
            }
            return latch.setCount(count);
        }
    }

    public void setCountDirect(String name, int count) {
        if (count < 0) {
            containers.remove(name);
        } else {
            CountDownLatchContainer latch = containers.get(name);
            if (latch == null) {
                latch = new CountDownLatchContainer(name);
                containers.put(name, latch);
            }
            latch.setCountDirect(count);
        }
    }

    public void countDown(String name) {
        CountDownLatchContainer latch = containers.get(name);
        if (latch != null) {
            if (latch.countDown() == 0) {
                containers.remove(name);
            }
        }
    }

    public boolean shouldWait(String name) {
        CountDownLatchContainer latch = containers.get(name);
        return latch != null && latch.getCount() > 0;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        containers.clear();
    }

    @Override
    public CountDownLatchProxy createDistributedObject(String name) {
        return new CountDownLatchProxy(name, nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
        quorumConfigCache.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Collection<CountDownLatchContainer> data = new LinkedList<CountDownLatchContainer>();
        for (Map.Entry<String, CountDownLatchContainer> latchEntry : containers.entrySet()) {
            String name = latchEntry.getKey();
            if (getPartitionId(name) == event.getPartitionId()) {
                CountDownLatchContainer value = latchEntry.getValue();
                data.add(value);
            }
        }
        return data.isEmpty() ? null : new CountDownLatchReplicationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int partitionId = event.getPartitionId();
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(partitionId);
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int partitionId = event.getPartitionId();
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(partitionId);
            }
        }
    }

    private int getPartitionId(String name) {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        return nodeEngine.getPartitionService().getPartitionId(partitionKey);
    }

    private void clearPartitionReplica(int partitionId) {
        final Iterator<String> iter = containers.keySet().iterator();
        while (iter.hasNext()) {
            String name = iter.next();
            if (getPartitionId(name) == partitionId) {
                iter.remove();
            }
        }
    }

    public CountDownLatchContainer getCountDownLatchContainer(String name) {
        return containers.get(name);
    }

    // need for testing..
    public boolean containsLatch(String name) {
        return containers.containsKey(name);
    }

    public void add(CountDownLatchContainer latch) {
        String name = latch.getName();
        containers.put(name, latch);
    }

    @Override
    public String getQuorumName(String name) {
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }
}
