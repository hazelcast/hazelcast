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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.operations.*;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @ali 1/2/13
 */
public abstract class MultiMapProxySupport extends AbstractDistributedObject {

    protected final String name;

    protected final CollectionService service;

    protected final MultiMapConfig config;

    protected final CollectionProxyType proxyType;

    protected MultiMapProxySupport(String name, CollectionService service, NodeEngine nodeEngine,
                                   CollectionProxyType proxyType, MultiMapConfig config) {
        super(nodeEngine);
        this.name = name;
        this.service = service;
        this.proxyType = proxyType;
        this.config = new MultiMapConfig(config);

    }

    public Object createNew() {
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)) {
            return new HashSet(10);
        } else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)) {
            return new LinkedList();
        }
        throw new IllegalArgumentException("No Matching CollectionProxyType!");
    }

    protected Boolean putInternal(Data dataKey, Data dataValue, int index) {
        try {
            PutOperation operation = new PutOperation(name, proxyType, dataKey, getThreadId(), dataValue, index);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected CollectionResponse getAllInternal(Data dataKey) {
        try {
            GetAllOperation operation = new GetAllOperation(name, proxyType, dataKey);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean removeInternal(Data dataKey, Data dataValue) {
        try {
            RemoveOperation operation = new RemoveOperation(name, proxyType, dataKey, getThreadId(), dataValue);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected CollectionResponse removeInternal(Data dataKey) {
        try {
            RemoveAllOperation operation = new RemoveAllOperation(name, proxyType, dataKey, getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Set<Data> localKeySetInternal() {
        return service.localKeySet(new CollectionProxyId(name, proxyType));
    }

    protected Set<Data> keySetInternal() {
        try {
            KeySetOperation operation = new KeySetOperation(name, proxyType);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                if (result == null) {
                    continue;
                }
                CollectionResponse response = (CollectionResponse) nodeEngine.toObject(result);
                keySet.addAll(response.getCollection());
            }
            return keySet;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Map valuesInternal() {
        try {
            ValuesOperation operation = new ValuesOperation(name, proxyType);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
            return results;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }


    protected Map entrySetInternal() {
        try {
            EntrySetOperation operation = new EntrySetOperation(name, proxyType);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
            return results;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean containsInternal(Data key, Data value) {
        try {
            ContainsEntryOperation operation = new ContainsEntryOperation(name, proxyType, key, value);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
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
            throw new HazelcastException(throwable);
        }
    }

    public int size() {
        try {
            SizeOperation operation = new SizeOperation(name, proxyType);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
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
            throw new HazelcastException(throwable);
        }
    }

    public void clear() {
        try {
            ClearOperation operation = new ClearOperation(name, proxyType);
            nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Integer countInternal(Data dataKey) {
        try {
            CountOperation operation = new CountOperation(name, proxyType, dataKey);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean lockInternal(Data dataKey, long timeout) {
        try {
            LockOperation operation = new LockOperation(name, proxyType, dataKey, getThreadId(), timeout);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean unlockInternal(Data dataKey) {
        try {
            UnlockOperation operation = new UnlockOperation(name, proxyType, dataKey, getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Object getInternal(Data dataKey, int index) {
        try {
            GetOperation operation = new GetOperation(name, proxyType, dataKey, index);
            return invokeData(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean containsInternalList(Data dataKey, Data dataValue) {
        try {
            ContainsOperation operation = new ContainsOperation(name, proxyType, dataKey, dataValue);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean containsAllInternal(Data dataKey, Set<Data> dataSet) {
        try {
            ContainsAllOperation operation = new ContainsAllOperation(name, proxyType, dataKey, dataSet);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Object setInternal(Data dataKey, int index, Data dataValue) {
        try {
            SetOperation operation = new SetOperation(name, proxyType, dataKey, getThreadId(), index, dataValue);
            return invokeData(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Object removeInternal(Data dataKey, int index) {
        try {
            RemoveIndexOperation operation = new RemoveIndexOperation(name, proxyType, dataKey, getThreadId(), index);
            return invokeData(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Integer indexOfInternal(Data dataKey, Data value, boolean last) {
        try {
            IndexOfOperation operation = new IndexOfOperation(name, proxyType, dataKey, value, last);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean addAllInternal(Data dataKey, List<Data> dataList, int index) {
        try {
            AddAllOperation operation = new AddAllOperation(name, proxyType, dataKey, getThreadId(), dataList, index);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Boolean compareAndRemoveInternal(Data dataKey, List<Data> dataList, boolean retain){
        try {
            CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, proxyType, dataKey, getThreadId(), dataList, retain);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return CollectionService.COLLECTION_SERVICE_NAME;
    }

    private <T> T invoke(CollectionOperation operation, Data dataKey) {
        try {
            int partitionId = nodeEngine.getPartitionId(dataKey);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CollectionService.COLLECTION_SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (T) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private Object invokeData(CollectionOperation operation, Data dataKey) {
        try {
            int partitionId = nodeEngine.getPartitionId(dataKey);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CollectionService.COLLECTION_SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private int getThreadId() {
        return ThreadContext.getThreadId();
    }

}
