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

package com.hazelcast.multimap;

import com.hazelcast.concurrent.lock.proxy.LockProxySupport;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.operations.*;
import com.hazelcast.multimap.operations.MultiMapOperationFactory.OperationFactoryType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author ali 1/2/13
 */
public abstract class MultiMapProxySupport extends AbstractDistributedObject<MultiMapService> {

    protected final MultiMapConfig config;
    protected final String name;
    protected final LockProxySupport lockSupport;

    protected MultiMapProxySupport(MultiMapService service, NodeEngine nodeEngine, String name) {
        super(nodeEngine, service);
        this.config = nodeEngine.getConfig().getMultiMapConfig(name);
        this.name = name;
        lockSupport = new LockProxySupport(new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name));
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

    protected Set<Data> localKeySetInternal() {
        return getService().localKeySet(name);
    }

    protected Set<Data> keySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {

            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.KEY_SET));
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                if (result == null) {
                    continue;
                }
                MultiMapResponse response = nodeEngine.toObject(result);
                if (response.getCollection() != null){
                    keySet.addAll(response.getCollection());
                }
            }
            return keySet;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map valuesInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.VALUES));
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map entrySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.ENTRY_SET));
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected boolean containsInternal(Data key, Data value) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.CONTAINS, key, value));
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
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.SIZE));
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
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            nodeEngine.getOperationService().invokeOnAllPartitions(MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.CLEAR));
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Integer countInternal(Data dataKey) {
        try {
            CountOperation operation = new CountOperation(name, dataKey);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

//    protected Object getInternal(Data dataKey, int index) {
//        try {
//            GetOperation operation = new GetOperation(proxyId, dataKey, index);
//            return invokeData(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Boolean containsInternalList(Data dataKey, Data dataValue) {
//        try {
//            ContainsOperation operation = new ContainsOperation(proxyId, dataKey, dataValue);
//            return invoke(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Boolean containsAllInternal(Data dataKey, Set<Data> dataSet) {
//        try {
//            ContainsAllOperation operation = new ContainsAllOperation(proxyId, dataKey, dataSet);
//            return invoke(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Object setInternal(Data dataKey, int index, Data dataValue) {
//        try {
//            SetOperation operation = new SetOperation(proxyId, dataKey, getThreadId(), index, dataValue);
//            return invokeData(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Object removeInternal(Data dataKey, int index) {
//        try {
//            RemoveIndexOperation operation = new RemoveIndexOperation(proxyId, dataKey, getThreadId(), index);
//            return invokeData(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Integer indexOfInternal(Data dataKey, Data value, boolean last) {
//        try {
//            IndexOfOperation operation = new IndexOfOperation(proxyId, dataKey, value, last);
//            return invoke(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Boolean addAllInternal(Data dataKey, List<Data> dataList, int index) {
//        try {
//            AddAllOperation operation = new AddAllOperation(proxyId, dataKey, getThreadId(), dataList, index);
//            return invoke(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }
//
//    protected Boolean compareAndRemoveInternal(Data dataKey, List<Data> dataList, boolean retain) {
//        try {
//            CompareAndRemoveOperation operation = new CompareAndRemoveOperation(proxyId, dataKey, getThreadId(), dataList, retain);
//            return invoke(operation, dataKey);
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }

    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    private <T> T invoke(Operation operation, Data dataKey) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MultiMapService.SERVICE_NAME, operation, partitionId).build();
            Future f;
            Object o;
            if (config.isStatisticsEnabled()) {
                long time = System.currentTimeMillis();
                f = invocation.invoke();
                o = f.get();
                if (operation instanceof PutOperation) {
                    getService().getLocalMultiMapStatsImpl(name).incrementPuts(System.currentTimeMillis() - time);    //TODO @ali should we remove statics from operations ?
                } else if (operation instanceof RemoveOperation || operation instanceof RemoveAllOperation) {
                    getService().getLocalMultiMapStatsImpl(name).incrementRemoves(System.currentTimeMillis() - time);
                } else if (operation instanceof GetAllOperation) {
                    getService().getLocalMultiMapStatsImpl(name).incrementGets(System.currentTimeMillis() - time);
                }
            } else {
                f = invocation.invoke();
                o = f.get();
            }
            return nodeEngine.toObject(o);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

//    private Object invokeData(CollectionOperation operation, Data dataKey) {
//        final NodeEngine nodeEngine = getNodeEngine();
//        try {
//            int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
//            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
//            Future f = inv.invoke();
//            return nodeEngine.toObject(f.get());
//        } catch (Throwable throwable) {
//            throw ExceptionUtil.rethrow(throwable);
//        }
//    }

    private int getThreadId() {
        return ThreadUtil.getThreadId();
    }

//    public static MultiMapConfig createConfig(CollectionProxyId proxyId) {
//        switch (proxyId.getType()) {
//            case MULTI_MAP:
//                return new MultiMapConfig().setName(proxyId.getName());
//            case LIST:
//                return new MultiMapConfig().setName(COLLECTION_LIST_NAME + proxyId.getKeyName())
//                    .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
//            case SET:
//                return new MultiMapConfig().setName(COLLECTION_SET_NAME + proxyId.getKeyName())
//                        .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
//            default:
//                throw new IllegalArgumentException("Illegal proxy type: " + proxyId.getType());
//        }
//    }
}
