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

package com.hazelcast.map.proxy;

import com.hazelcast.concurrent.lock.LockProxySupport;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.*;
import com.hazelcast.map.operation.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.util.*;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @author enesakar 1/17/13
 */
abstract class MapProxySupport extends AbstractDistributedObject<MapService> implements InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    protected final String name;
    protected final MapConfig mapConfig;
    protected final LocalMapStatsImpl localMapStats;
    protected final LockProxySupport lockSupport;
    protected final PartitioningStrategy partitionStrategy;

    protected MapProxySupport(final String name, final MapService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;
        mapConfig = service.getMapContainer(name).getMapConfig();
        partitionStrategy = service.getMapContainer(name).getPartitioningStrategy();
        localMapStats = service.getLocalMapStatsImpl(name);
        lockSupport = new LockProxySupport(new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    @Override
    public void initialize() {
        initializeListeners();
        initializeIndexes();
        initializeMapStoreLoad();

    }

    private void initializeMapStoreLoad() {
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
            MapStoreConfig.InitialLoadMode initialLoadMode = mapStoreConfig.getInitialLoadMode();
            if (initialLoadMode.equals(MapStoreConfig.InitialLoadMode.EAGER)) {
                waitUntilLoaded();
            }
        }
    }

    private void initializeIndexes() {
        for (MapIndexConfig index : mapConfig.getMapIndexConfigs()) {
            if (index.getAttribute() != null) {
                addIndex(index.getAttribute(), index.isOrdered());
            }
        }
    }

    private void initializeListeners() {
        final NodeEngine nodeEngine = getNodeEngine();
        List<EntryListenerConfig> listenerConfigs = mapConfig.getEntryListenerConfigs();
        for (EntryListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil
                            .newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                if (listenerConfig.isLocal()) {
                    addLocalEntryListener(listener);
                } else {
                    addEntryListenerInternal(listener, null, listenerConfig.isIncludeValue());
                }
            }
        }
    }

    // this operation returns the object in data format except it is got from near-cache and near-cache memory format is object.
    protected Object getInternal(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapConfig.isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getFromNearCache(name, key);
            if (cached != null) {
                if (NearCache.NULL_OBJECT.equals(cached)) {
                    cached = null;
                }
                mapService.interceptAfterGet(name, cached);
                return cached;
            }
        }
        NodeEngine nodeEngine = getNodeEngine();
        // todo action for read-backup true is not well tested.
        if (mapConfig.isReadBackupData()) {
            int backupCount = mapConfig.getTotalBackupCount();
            InternalPartitionService partitionService = mapService.getNodeEngine().getPartitionService();
            for (int i = 0; i <= backupCount; i++) {
                int partitionId = partitionService.getPartitionId(key);
                InternalPartition partition = partitionService.getPartition(partitionId);
                if (nodeEngine.getThisAddress().equals(partition.getReplicaAddress(i))) {
                    Object val = mapService.getPartitionContainer(partitionId).getRecordStore(name).get(key);
                    if (val != null) {
                        mapService.interceptAfterGet(name, val);
                        // this serialization step is needed not to expose the object, see issue 1292
                        return mapService.toData(val);
                    }
                }
            }
        }
        GetOperation operation = new GetOperation(name, key);
        Data result = (Data) invokeOperation(key, operation);
        if (nearCacheEnabled) {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            if (!nodeEngine.getPartitionService().getPartitionOwner(partitionId)
                    .equals(nodeEngine.getClusterService().getThisAddress()) || mapConfig.getNearCacheConfig().isCacheLocalEntries()) {
                mapService.putNearCache(name, key, result);
            }
        }
        return result;
    }

    protected ICompletableFuture<Data> getAsyncInternal(final Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        final boolean nearCacheEnabled = mapConfig.isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getFromNearCache(name, key);
            if (cached != null) {
                if (NearCache.NULL_OBJECT.equals(cached)) {
                    cached = null;
                }
                return new CompletedFuture<Data>(
                        nodeEngine.getSerializationService(),
                        cached,
                        nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
            }
        }

        GetOperation operation = new GetOperation(name, key);
        try {
            return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Data putInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return previousValue;
    }

    protected boolean tryPutInternal(final Data key, final Data value, final long timeout, final TimeUnit timeunit) {
        TryPutOperation operation = new TryPutOperation(name, key, value, getTimeInMillis(timeout, timeunit));
        boolean putSuccessful = (Boolean) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return putSuccessful;
    }

    protected Data putIfAbsentInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutIfAbsentOperation operation = new PutIfAbsentOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return previousValue;
    }

    protected void putTransientInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutTransientOperation operation = new PutTransientOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        invokeOperation(key, operation);
        invalidateLocalNearCache(key);
    }

    private Object invokeOperation(Data key, KeyBasedMapOperation operation) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Future f;
            Object o;
            OperationService operationService = nodeEngine.getOperationService();
            if (mapConfig.isStatisticsEnabled()) {
                long time = System.currentTimeMillis();
                f = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false)
                        .invoke();
                o = f.get();
                if (operation instanceof BasePutOperation)
                    localMapStats.incrementPuts(System.currentTimeMillis() - time);
                else if (operation instanceof BaseRemoveOperation)
                    localMapStats.incrementRemoves(System.currentTimeMillis() - time);
                else if (operation instanceof GetOperation)
                    localMapStats.incrementGets(System.currentTimeMillis() - time);

            } else {
                f = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false).invoke();
                o = f.get();
            }
            return o;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected ICompletableFuture<Data> putAsyncInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            invalidateLocalNearCache(key);
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected boolean replaceInternal(final Data key, final Data oldValue, final Data newValue) {
        ReplaceIfSameOperation operation = new ReplaceIfSameOperation(name, key, oldValue, newValue);
        boolean replaceSuccessful = (Boolean) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return replaceSuccessful;
    }

    protected Data replaceInternal(final Data key, final Data value) {
        ReplaceOperation operation = new ReplaceOperation(name, key, value);
        final Data result = (Data) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return result;
    }

    protected void setInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        SetOperation operation = new SetOperation(name, key, value, timeunit.toMillis(ttl));
        invokeOperation(key, operation);
        invalidateLocalNearCache(key);
    }

    protected boolean evictInternal(final Data key) {
        EvictOperation operation = new EvictOperation(name, key, false);
        final boolean evictSuccess = (Boolean) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return evictSuccess;
    }

    protected Data removeInternal(Data key) {
        RemoveOperation operation = new RemoveOperation(name, key);
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return previousValue;
    }

    protected void deleteInternal(Data key) {
        RemoveOperation operation = new RemoveOperation(name, key);
        invokeOperation(key, operation);
        invalidateLocalNearCache(key);
    }

    protected boolean removeInternal(final Data key, final Data value) {
        RemoveIfSameOperation operation = new RemoveIfSameOperation(name, key, value);
        boolean removed = (Boolean) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return removed;
    }

    protected boolean tryRemoveInternal(final Data key, final long timeout, final TimeUnit timeunit) {
        TryRemoveOperation operation = new TryRemoveOperation(name, key, getTimeInMillis(timeout, timeunit));
        boolean removed = (Boolean) invokeOperation(key, operation);
        invalidateLocalNearCache(key);
        return removed;
    }

    protected ICompletableFuture<Data> removeAsyncInternal(final Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        RemoveOperation operation = new RemoveOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            invalidateLocalNearCache(key);
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected boolean containsKeyInternal(Data key) {
        if (isKeyInNearCache(key)) {
            return true;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        ContainsKeyOperation containsKeyOperation = new ContainsKeyOperation(name, key);
        containsKeyOperation.setServiceName(SERVICE_NAME);
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, containsKeyOperation,
                    partitionId);
            return (Boolean) getService().toObject(f.get());
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void waitUntilLoaded() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new PartitionCheckIfLoadedOperationFactory(name));
            Iterator<Entry<Integer, Object>> iterator = results.entrySet().iterator();
            boolean isFinished = false;
            final Set<Integer> retrySet = new HashSet<Integer>();
            while (!isFinished) {
                while (iterator.hasNext()) {
                    final Entry<Integer, Object> entry = iterator.next();
                    if (Boolean.TRUE.equals(entry.getValue())) {
                        iterator.remove();
                    } else {
                        retrySet.add(entry.getKey());
                    }
                }
                if (retrySet.size() > 0) {
                    results = retryPartitions(retrySet);
                    iterator = results.entrySet().iterator();
                    Thread.sleep(1000);
                    retrySet.clear();
                } else {
                    isFinished = true;
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private Map<Integer, Object> retryPartitions(Collection partitions) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, new PartitionCheckIfLoadedOperationFactory(name), partitions);
            return results;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

    }

    public int size() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) getService().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean containsValueInternal(Data dataValue) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new ContainsValueOperationFactory(name, dataValue));
            for (Object result : results.values()) {
                Boolean contains = (Boolean) getService().toObject(result);
                if (contains)
                    return true;
            }
            return false;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean isEmpty() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapIsEmptyOperation(name), nodeEngine));
            for (Object result : results.values()) {
                if (!(Boolean) getService().toObject(result))
                    return false;
            }
            return true;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Map<Object, Object> getAllObjectInternal(final Set<Data> keys) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        Map<Object, Object> result = new HashMap<Object, Object>();
        final boolean nearCacheEnabled = mapConfig.isNearCacheEnabled();
        if (nearCacheEnabled) {
            final Iterator<Data> iterator = keys.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cachedValue = mapService.getFromNearCache(name, key);
                if (cachedValue != null) {
                    if (!NearCache.NULL_OBJECT.equals(cachedValue)) {
                        result.put(key, cachedValue);
                    }
                    iterator.remove();
                }
            }
        }
        if (keys.isEmpty()) {
            return result;
        }
        Collection<Integer> partitions = getPartitionsForKeys(keys);
        Map<Integer, Object> responses = null;
        try {
            responses = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, new MapGetAllOperationFactory(name, keys), partitions);
            for (Object response : responses.values()) {
                Set<Map.Entry<Data, Data>> entries = ((MapEntrySet) mapService.toObject(response)).getEntrySet();
                for (Entry<Data, Data> entry : entries) {
                    result.put(mapService.toObject(entry.getKey()), mapService.toObject(entry.getValue()));
                    if (nearCacheEnabled) {
                        int partitionId = nodeEngine.getPartitionService().getPartitionId(entry.getKey());
                        if (!nodeEngine.getPartitionService().getPartitionOwner(partitionId)
                                .equals(nodeEngine.getClusterService().getThisAddress()) || mapConfig.getNearCacheConfig().isCacheLocalEntries()) {
                            mapService.putNearCache(name, entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return result;
    }

    private Collection<Integer> getPartitionsForKeys(Set<Data> keys) {
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitions = partitionService.getPartitionCount();
        int capacity = Math.min(partitions, keys.size()); //todo: is there better way to estimate size?
        Set<Integer> partitionIds = new HashSet<Integer>(capacity);

        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
    }

    protected void putAllInternal(final Map<? extends Object, ? extends Object> entries) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        int factor = 3;
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();
        int partitionCount = partitionService.getPartitionCount();
        boolean tooManyEntries = entries.size() > (partitionCount * factor);
        try {
            if (tooManyEntries) {
                List<Future> futures = new LinkedList<Future>();
                Map<Integer, MapEntrySet> entryMap = new HashMap<Integer, MapEntrySet>(nodeEngine.getPartitionService().getPartitionCount());
                for (Entry entry : entries.entrySet()) {
                    if (entry.getKey() == null) {
                        throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
                    }
                    if (entry.getValue() == null) {
                        throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
                    }
                    int partitionId = partitionService.getPartitionId(entry.getKey());
                    if (!entryMap.containsKey(partitionId)) {
                        entryMap.put(partitionId, new MapEntrySet());
                    }
                    entryMap.get(partitionId).add(new AbstractMap.SimpleImmutableEntry<Data, Data>(mapService.toData(
                            entry.getKey(),
                            partitionStrategy),
                            mapService
                                    .toData(entry.getValue())
                    ));
                }

                for (final Map.Entry<Integer, MapEntrySet> entry : entryMap.entrySet()) {
                    final Integer partitionId = entry.getKey();
                    final PutAllOperation op = new PutAllOperation(name, entry.getValue());
                    op.setPartitionId(partitionId);
                    futures.add(operationService.invokeOnPartition(SERVICE_NAME, op, partitionId));
                }

                for (Future future : futures) {
                    future.get();
                }

            } else {
                for (Entry entry : entries.entrySet()) {
                    if (entry.getKey() == null) {
                        throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
                    }
                    if (entry.getValue() == null) {
                        throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
                    }
                    putInternal(mapService.toData(entry.getKey(), partitionStrategy),
                            mapService.toData(entry.getValue()),
                            -1,
                            TimeUnit.SECONDS);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected Set<Data> keySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            // todo you can optimize keyset by taking keys without lock then re-fetch missing ones. see localKeySet
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapKeySetOperation(name), nodeEngine));
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set keys = ((MapKeySet) getService().toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set<Data> localKeySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        Set<Data> keySet = new HashSet<Data>();
        try {
            List<Integer> memberPartitions =
                    nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
            for (Integer memberPartition : memberPartitions) {
                RecordStore recordStore = mapService.getRecordStore(memberPartition, name);
                keySet.addAll(recordStore.getReadonlyRecordMap().keySet());
            }
            return keySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void flush() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            // todo add a feature to mancenter to sync cache to db completely
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapFlushOperation(name), nodeEngine));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Collection<Data> valuesInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapValuesOperation(name), nodeEngine));
            List<Data> values = new ArrayList<Data>();
            for (Object result : results.values()) {
                values.addAll(((MapValueCollection) getService().toObject(result)).getValues());
            }
            return values;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void clearInternal() {
        final String mapName = name;
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            ClearOperation clearOperation = new ClearOperation(mapName);
            clearOperation.setServiceName(SERVICE_NAME);
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(clearOperation, nodeEngine));

        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public String addMapInterceptorInternal(MapInterceptor interceptor) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        if (interceptor instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) interceptor).setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
        String id = mapService.addInterceptor(name, interceptor);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                if (member.localMember())
                    continue;
                Future f = nodeEngine.getOperationService()
                        .invokeOnTarget(SERVICE_NAME, new AddInterceptorOperation(id, interceptor, name),
                                member.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return id;
    }

    public void removeMapInterceptorInternal(String id) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        mapService.removeInterceptor(name, id);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (Member member : members) {
            try {
                if (member.localMember())
                    continue;
                MemberImpl memberImpl = (MemberImpl) member;
                Future f = nodeEngine.getOperationService()
                        .invokeOnTarget(SERVICE_NAME, new RemoveInterceptorOperation(name, id), memberImpl.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public String addLocalEntryListener(final EntryListener listener) {
        final MapService mapService = getService();
        return mapService.addLocalEventListener(listener, name);
    }

    public String addLocalEntryListenerInternal(EntryListener listener, Predicate predicate, final Data key, boolean includeValue) {
        final MapService mapService = getService();
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        return mapService.addLocalEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(
            final EntryListener listener, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new EntryEventFilter(includeValue, key);
        final MapService mapService = getService();
        return mapService.addEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(
            EntryListener listener, Predicate predicate, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        final MapService mapService = getService();
        return mapService.addEventListener(listener, eventFilter, name);
    }

    protected boolean removeEntryListenerInternal(String id) {
        final MapService mapService = getService();
        return mapService.removeEventListener(name, id);
    }

    protected EntryView getEntryViewInternal(final Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        GetEntryViewOperation getEntryViewOperation = new GetEntryViewOperation(name, key);
        getEntryViewOperation.setServiceName(SERVICE_NAME);
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, getEntryViewOperation, partitionId);
            Object o = getService().toObject(f.get());
            return (EntryView) o;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected Set<Entry<Data, Data>> entrySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapEntrySetOperation(name), nodeEngine));
            Set<Entry<Data, Data>> entrySet = new HashSet<Entry<Data, Data>>();
            for (Object result : results.values()) {
                Set entries = ((MapEntrySet) getService().toObject(result)).getEntrySet();
                if (entries != null)
                    entrySet.addAll(entries);
            }
            return entrySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Future future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            return (Data) future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        Map result = new HashMap();
        final NodeEngine nodeEngine = getNodeEngine();
        final Collection<Integer> partitionsForKeys = getPartitionsForKeys(keys);
        try {
            MultipleEntryOperationFactory operationFactory = new MultipleEntryOperationFactory(name, keys, entryProcessor);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, operationFactory, partitionsForKeys);
            for (Object o : results.values()) {
                if (o != null) {
                    final MapService service = getService();
                    final MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        result.put(service.toObject(entry.getKey()), service.toObject(entry.getValue()));
                    }
                }
            }

        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    public ICompletableFuture executeOnKeyInternal(Data key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            if (callback == null) {
                return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            } else {
                return nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setCallback(new MapExecutionCallbackAdapter(callback))
                        .invoke();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    //todo: dead code??
    public Map executeOnEntries(EntryProcessor entryProcessor) {
        Map result = new HashMap();
        try {
            NodeEngine nodeEngine = getNodeEngine();
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new PartitionWideEntryOperationFactory(name, entryProcessor));
            for (Object o : results.values()) {
                if (o != null) {
                    final MapService service = getService();
                    final MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        result.put(service.toObject(entry.getKey()), service.toObject(entry.getValue()));
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    //todo: dead code??
    public Map executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        Map result = new HashMap();
        try {
            NodeEngine nodeEngine = getNodeEngine();
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new PartitionWideEntryWithPredicateOperationFactory(name,
                                    entryProcessor,
                                    predicate)
                    );
            for (Object o : results.values()) {
                if (o != null) {
                    final MapService service = getService();
                    final MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        result.put(service.toObject(entry.getKey()), service.toObject(entry.getValue()));
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    protected Set queryLocal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        final SerializationService ss = nodeEngine.getSerializationService();
        List<Integer> partitionIds = nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(iterationType);
            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                query(pagingPredicate, iterationType, dataResult);
                pagingPredicate.nextPage();
            }
        }
        Set result;
        if (pagingPredicate == null) {
            result = new QueryResultSet(ss, iterationType, dataResult);
        } else {
            result = new SortedQueryResultSet(pagingPredicate.getComparator(), iterationType, pagingPredicate.getPageSize());
        }

        List<Integer> returnedPartitionIds = new ArrayList<Integer>();
        try {
            Future future = operationService
                    .invokeOnTarget(SERVICE_NAME,
                            new QueryOperation(name, predicate),
                            nodeEngine.getThisAddress());
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                returnedPartitionIds = queryResult.getPartitionIds();
                if (pagingPredicate == null) {
                    result.addAll(queryResult.getResult());
                } else {
                    for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                        Object key = ss.toObject(queryResultEntry.getKeyData());
                        Object value = ss.toObject(queryResultEntry.getValueData());
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }

            if (returnedPartitionIds.size() == partitionIds.size()) {
                if (pagingPredicate != null) {
                    PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                }
                return result;
            }
            List<Integer> missingList = new ArrayList<Integer>();
            for (Integer partitionId : partitionIds) {
                if (!returnedPartitionIds.contains(partitionId))
                    missingList.add(partitionId);
            }
            List<Future> futures = new ArrayList<Future>(missingList.size());
            for (Integer pid : missingList) {
                QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
                queryPartitionOperation.setPartitionId(pid);
                try {
                    Future f =
                            operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, pid);
                    futures.add(f);
                } catch (Throwable t) {
                    throw ExceptionUtil.rethrow(t);
                }
            }
            for (Future f : futures) {
                QueryResult qResult = (QueryResult) f.get();
                if (pagingPredicate == null) {
                    result.addAll(qResult.getResult());
                } else {
                    for (QueryResultEntry queryResultEntry : qResult.getResult()) {
                        Object key = ss.toObject(queryResultEntry.getKeyData());
                        Object value = ss.toObject(queryResultEntry.getValueData());
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }


    protected Set query(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {

        final NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        final SerializationService ss = nodeEngine.getSerializationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> plist = new HashSet<Integer>(partitionCount);
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(iterationType);
            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                query(pagingPredicate, iterationType, dataResult);
                pagingPredicate.nextPage();
            }
        }
        Set result;
        if (pagingPredicate == null) {
            result = new QueryResultSet(ss, iterationType, dataResult);
        } else {
            result = new SortedQueryResultSet(pagingPredicate.getComparator(), iterationType, pagingPredicate.getPageSize());
        }
        List<Integer> missingList = new ArrayList<Integer>();
        try {
            List<Future> flist = new ArrayList<Future>();
            for (MemberImpl member : members) {
                Future future = operationService
                        .invokeOnTarget(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress());
                flist.add(future);
            }
            for (Future future : flist) {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    final List<Integer> partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null) {
                        plist.addAll(partitionIds);
                        if (pagingPredicate == null) {
                            result.addAll(queryResult.getResult());
                        } else {
                            for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                                Object key = ss.toObject(queryResultEntry.getKeyData());
                                Object value = ss.toObject(queryResultEntry.getValueData());
                                result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                            }
                        }
                    }
                }
            }
            if (plist.size() == partitionCount) {
                if (pagingPredicate != null) {
                    PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                }
                return result;
            }
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        } catch (Throwable t) {
            missingList.clear();
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        }

        try {
            List<Future> futures = new ArrayList<Future>(missingList.size());
            for (Integer pid : missingList) {
                QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
                queryPartitionOperation.setPartitionId(pid);
                try {
                    Future f =
                            operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, pid);
                    futures.add(f);
                } catch (Throwable t) {
                    throw ExceptionUtil.rethrow(t);
                }
            }
            for (Future future : futures) {
                QueryResult queryResult = (QueryResult) future.get();
                if (pagingPredicate == null) {
                    result.addAll(queryResult.getResult());
                } else {
                    for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                        Object key = ss.toObject(queryResultEntry.getKeyData());
                        Object value = ss.toObject(queryResultEntry.getValueData());
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        if (pagingPredicate != null) {
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
        }
        return result;
    }

    public void addIndex(final String attribute, final boolean ordered) {
        final NodeEngine nodeEngine = getNodeEngine();
        if (attribute == null) throw new IllegalArgumentException("Attribute name cannot be null");
        try {
            AddIndexOperation addIndexOperation = new AddIndexOperation(name, attribute, ordered);
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(addIndexOperation, nodeEngine));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public LocalMapStats getLocalMapStats() {
        return getService().createLocalMapStats(name);
    }

    private boolean isKeyInNearCache(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapConfig.isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getFromNearCache(name, key);
            if (cached != null && !cached.equals(NearCache.NULL_OBJECT)) {
                return true;
            }
        }
        return false;
    }

    private void invalidateLocalNearCache(Data key) {
        // invalidate local near cache to ensure this thread does not see its old state
        // note: this is a local-only operation and therefore fast and safe to execute
        final MapConfig config = mapConfig;
        final boolean nearCacheEnabled = config.isNearCacheEnabled();
        if (!nearCacheEnabled) {
            return;
        }
        final boolean cacheLocalEntries = config.getNearCacheConfig().isCacheLocalEntries();
        if (!cacheLocalEntries) {
            return;
        }
        final MapService mapService = getService();
        mapService.invalidateNearCache(name, key);
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public final String getName() {
        return name;
    }

    public final String getServiceName() {
        return SERVICE_NAME;
    }

    private class MapExecutionCallbackAdapter implements Callback {

        private final ExecutionCallback executionCallback;

        public MapExecutionCallbackAdapter(ExecutionCallback executionCallback) {
            this.executionCallback = executionCallback;
        }

        @Override
        public void notify(Object response) {
            if (response instanceof Throwable) {
                executionCallback.onFailure((Throwable) response);
            } else {
                executionCallback.onResponse(getService().toObject(response));
            }
        }

    }

}
