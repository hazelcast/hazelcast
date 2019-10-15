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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.locksupport.LockProxySupport;
import com.hazelcast.internal.locksupport.LockSupportServiceImpl;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.DeleteOperation;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory.OperationFactoryType;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllOperation;
import com.hazelcast.multimap.impl.operations.RemoveOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public abstract class MultiMapProxySupport extends AbstractDistributedObject<MultiMapService> {

    protected final MultiMapConfig config;
    protected final String name;
    protected final LockProxySupport lockSupport;

    protected MultiMapProxySupport(MultiMapConfig config, MultiMapService service, NodeEngine nodeEngine, String name) {
        super(nodeEngine, service);
        this.config = config;
        this.name = name;

        lockSupport = new LockProxySupport(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name),
                LockSupportServiceImpl.getMaxLeaseTimeInMillis(nodeEngine.getProperties()));
    }

    @Override
    public String getName() {
        return name;
    }

    protected Boolean putInternal(Data dataKey, Data dataValue, int index) {
        try {
            PutOperation operation = new PutOperation(name, dataKey, getThreadId(), dataValue, index);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected MultiMapResponse getAllInternal(Data dataKey) {
        try {
            GetAllOperation operation = new GetAllOperation(name, dataKey);
            operation.setThreadId(ThreadUtil.getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Boolean removeInternal(Data dataKey, Data dataValue) {
        try {
            RemoveOperation operation = new RemoveOperation(name, dataKey, getThreadId(), dataValue);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected MultiMapResponse removeInternal(Data dataKey) {
        try {
            RemoveAllOperation operation = new RemoveAllOperation(name, dataKey, getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected void deleteInternal(Data dataKey) {
        try {
            DeleteOperation operation = new DeleteOperation(name, dataKey, getThreadId());
            invoke(operation, dataKey);
        } catch (Throwable throwable) {
              throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Set<Data> localKeySetInternal() {
        return getService().localKeySet(name);
    }

    protected Set<Data> keySetInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {

            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.KEY_SET)
                    );
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                if (result == null) {
                    continue;
                }
                MultiMapResponse response = nodeEngine.toObject(result);
                if (response.getCollection() != null) {
                    keySet.addAll(response.getCollection());
                }
            }
            return keySet;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map valuesInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.VALUES)
                    );
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map entrySetInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.ENTRY_SET)
                    );
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected boolean containsInternal(Data key, Data value) {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.CONTAINS,
                                    key, value, ThreadUtil.getThreadId())
                    );
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Boolean result = nodeEngine.toObject(obj);
                if (result) {
                    return true;
                }
            }
            return false;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public int size() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.SIZE)
                    );
            int size = 0;
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Integer result = nodeEngine.toObject(obj);
                size += result;
            }
            return size;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public void clear() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> resultMap = nodeEngine.getOperationService().invokeOnAllPartitions(
                    MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.CLEAR)
            );

            int numberOfAffectedEntries = 0;
            for (Object o : resultMap.values()) {
                numberOfAffectedEntries += (Integer) o;
            }
            publishMultiMapEvent(numberOfAffectedEntries, EntryEventType.CLEAR_ALL);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private void publishMultiMapEvent(int numberOfAffectedEntries, EntryEventType eventType) {
        getService().publishMultiMapEvent(name, eventType, numberOfAffectedEntries);
    }

    protected Integer countInternal(Data dataKey) {
        try {
            CountOperation operation = new CountOperation(name, dataKey);
            operation.setThreadId(ThreadUtil.getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    private <T> T invoke(Operation operation, Data dataKey) {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
            Future future;
            Object result;
            if (config.isStatisticsEnabled()) {
                long startTimeNanos = System.nanoTime();
                future = nodeEngine.getOperationService()
                        .invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                result = future.get();
                if (operation instanceof PutOperation) {
                    // TODO: @ali should we remove statics from operations?
                    getService().getLocalMultiMapStatsImpl(name).incrementPutLatencyNanos(System.nanoTime() - startTimeNanos);
                } else if (operation instanceof RemoveOperation || operation instanceof RemoveAllOperation
                        || operation instanceof DeleteOperation) {
                    getService().getLocalMultiMapStatsImpl(name).incrementRemoveLatencyNanos(System.nanoTime() - startTimeNanos);
                } else if (operation instanceof GetAllOperation) {
                    getService().getLocalMultiMapStatsImpl(name).incrementGetLatencyNanos(System.nanoTime() - startTimeNanos);
                }
            } else {
                future = nodeEngine.getOperationService()
                        .invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                result = future.get();
            }
            return nodeEngine.toObject(result);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private long getThreadId() {
        return ThreadUtil.getThreadId();
    }

    @Override
    public String toString() {
        return "MultiMap{name=" + name + '}';
    }
}
