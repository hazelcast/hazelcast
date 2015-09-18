/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.map.impl.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link com.hazelcast.core.IMap} interface.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 */
public class MapProxyImpl<K, V> extends MapProxySupport implements IMap<K, V>, InitializingObject {

    public MapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    @Override
    public V get(Object k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        return (V) toObject(getInternal(key));
    }

    @Override
    public V put(K k, V v) {
        return put(k, v, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V put(K k, V v, long ttl, TimeUnit timeunit) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        Data result = putInternal(key, value, ttl, timeunit);
        return (V) toObject(result);
    }

    @Override
    public boolean tryPut(K k, V v, long timeout, TimeUnit timeunit) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        return tryPutInternal(key, value, timeout, timeunit);
    }

    @Override
    public V putIfAbsent(K k, V v) {
        return putIfAbsent(k, v, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V putIfAbsent(K k, V v, long ttl, TimeUnit timeunit) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        Data result = putIfAbsentInternal(key, value, ttl, timeunit);
        return (V) toObject(result);
    }

    @Override
    public void putTransient(K k, V v, long ttl, TimeUnit timeunit) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        putTransientInternal(key, value, ttl, timeunit);
    }

    @Override
    public boolean replace(K k, V o, V v) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(o, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data oldValue = toData(o);
        Data value = toData(v);
        return replaceInternal(key, oldValue, value);
    }

    @Override
    public V replace(K k, V v) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        return (V) toObject(replaceInternal(key, value));
    }

    @Override
    public void set(K key, V value) {
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(K k, V v, long ttl, TimeUnit timeunit) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        setInternal(key, value, ttl, timeunit);
    }

    @Override
    public V remove(Object k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data result = removeInternal(key);
        return (V) toObject(result);
    }

    @Override
    public boolean remove(Object k, Object v) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        Data value = toData(v);
        return removeInternal(key, value);
    }

    @Override
    public void delete(Object k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        deleteInternal(key);
    }

    @Override
    public boolean containsKey(Object k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        return containsKeyInternal(key);
    }

    @Override
    public boolean containsValue(Object v) {
        checkNotNull(v, NULL_VALUE_IS_NOT_ALLOWED);

        Data value = toData(v);
        return containsValueInternal(value);
    }

    @Override
    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data k = toData(key, partitionStrategy);
        lockSupport.lock(nodeEngine, k);
    }

    @Override
    public void lock(Object key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive(leaseTime, "leaseTime should be positive");

        Data k = toData(key, partitionStrategy);
        lockSupport.lock(getNodeEngine(), k, timeUnit.toMillis(leaseTime));
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data k = toData(key, partitionStrategy);
        lockSupport.unlock(nodeEngine, k);
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        return tryRemoveInternal(k, timeout, timeunit);
    }

    @Override
    public Future<V> getAsync(K k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        NodeEngine nodeEngine = getNodeEngine();
        return new DelegatingFuture<V>(getAsyncInternal(key), nodeEngine.getSerializationService());
    }

    @Override
    public boolean isLocked(K k) {
        checkNotNull(k, NULL_KEY_IS_NOT_ALLOWED);

        Data key = toData(k, partitionStrategy);
        NodeEngine nodeEngine = getNodeEngine();
        return lockSupport.isLocked(nodeEngine, key);
    }

    @Override
    public Future putAsync(K key, V value) {
        return putAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        Data v = toData(value);
        return new DelegatingFuture<V>(putAsyncInternal(k, v, ttl, timeunit),
                getNodeEngine().getSerializationService());
    }

    @Override
    public ICompletableFuture removeAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        return new DelegatingFuture<V>(removeAsyncInternal(k), getNodeEngine().getSerializationService());
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Set<Data> ks = new HashSet(keys.size());
        for (K key : keys) {
            checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

            Data k = toData(key, partitionStrategy);
            ks.add(k);
        }
        return (Map<K, V>) getAllObjectInternal(ks);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        // Note, putAllInternal() will take care of the null key/value checks.
        putAllInternal(m);
    }

    @Override
    public boolean tryLock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        return lockSupport.tryLock(getNodeEngine(), k);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        return lockSupport.tryLock(getNodeEngine(), k, time, timeunit);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit,
                           long leaseTime, TimeUnit leaseTimeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        return lockSupport.tryLock(getNodeEngine(), k, time, timeunit, leaseTime, leaseTimeunit);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data k = toData(key, partitionStrategy);
        lockSupport.forceUnlock(getNodeEngine(), k);
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        checkNotNull(interceptor, "Interceptor should not be null!");

        return addMapInterceptorInternal(interceptor);
    }

    @Override
    public void removeInterceptor(String id) {
        checkNotNull(id, "Interceptor id should not be null!");

        removeMapInterceptorInternal(id);
    }

    @Override
    public String addLocalEntryListener(MapListener listener) {
        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener) {
        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        Data keyData = toData(key, partitionStrategy);
        return addLocalEntryListenerInternal(listener, predicate, keyData, includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        Data keyData = toData(key, partitionStrategy);
        return addLocalEntryListenerInternal(listener, predicate, keyData, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, toData(key, partitionStrategy), includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, toData(key, partitionStrategy), includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, predicate, toData(key, partitionStrategy), includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, predicate, toData(key, partitionStrategy), includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public boolean removeEntryListener(String id) {
        checkNotNull(id, "Listener id should not be null!");

        return removeEntryListenerInternal(id);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        return addPartitionLostListenerInternal(listener);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        checkNotNull(id, "Listener id should not be null!");

        return removePartitionLostListenerInternal(id);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        SimpleEntryView<K, V> entryViewInternal = (SimpleEntryView) getEntryViewInternal(toData(key, partitionStrategy));
        if (entryViewInternal == null) {
            return null;
        }
        Data value = (Data) entryViewInternal.getValue();
        entryViewInternal.setKey(key);
        entryViewInternal.setValue((V) toObject(value));
        return entryViewInternal;
    }

    @Override
    public boolean evict(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return evictInternal(toData(key, partitionStrategy));
    }

    @Override
    public void evictAll() {
        evictAllInternal();
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        checkNotNull(getMapStore(), "First you should configure a map store");

        loadAllInternal(replaceExistingValues);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        checkNotNull(getMapStore(), "First you should configure a map store");
        checkNotNull(keys, "Parameter keys should not be null.");

        loadInternal(keys, replaceExistingValues);
    }

    /**
     * This method clears the map and deletaAll on MapStore which if connected to a database,
     * will delete the records from that database.
     * <p/>
     * If you wish to clear the map only without calling deleteAll, use
     *
     * @see #clearMapOnly
     */
    @Override
    public void clear() {
        clearInternal();
    }

    /**
     * This method clears the map.It does not invoke deleteAll on any associated MapStore.
     *
     * @see #clear
     */
    //TODO: Why is this not tested
    //TODO: how come the implementation is the same as clear? I think this code is broken.
    //TODO: This method also isn't part of the IMap API.
    public void clearMapOnly() {
        //need a different method here that does not call deleteAll
        clearInternal();
    }

    @Override
    public Set<K> keySet() {
        return keySet(TruePredicate.INSTANCE);
    }

    @Override
    public Collection<V> values() {
        return values(TruePredicate.INSTANCE);
    }

    @Override
    public Set entrySet() {
        return entrySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return (Set) query(predicate, IterationType.KEY, false, true);
    }

    @Override
    public Set entrySet(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return (Set) query(predicate, IterationType.ENTRY, false, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return query(predicate, IterationType.VALUE, false, false);
    }

    @Override
    public Set<K> localKeySet() {
        return localKeySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> localKeySet(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        return queryLocal(predicate, IterationType.KEY, false);
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data result = executeOnKeyInternal(toData(key, partitionStrategy), entryProcessor);
        return toObject(result);
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        if (keys == null || keys.size() == 0) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Set<Data> dataKeys = new HashSet<Data>(keys.size());
        for (K key : keys) {
            dataKeys.add(toData(key, partitionStrategy));
        }
        return executeOnKeysInternal(dataKeys, entryProcessor);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key, partitionStrategy);
        executeOnKeyInternal(keyData, entryProcessor, callback);
    }

    @Override
    public ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        MapService service = getService();
        Data keyData = toData(key, partitionStrategy);
        ICompletableFuture f = executeOnKeyInternal(keyData, entryProcessor, null);
        return new DelegatingFuture(f, service.getMapServiceContext().getNodeEngine().getSerializationService());
    }


    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {

        HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-map-" + getName());
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {

        try {
            isNotNull(jobTracker, "jobTracker");
            KeyValueSource<K, V> keyValueSource = KeyValueSource.fromMap(this);
            Job<K, V> job = jobTracker.newJob(keyValueSource);
            Mapper mapper = aggregation.getMapper(supplier);
            CombinerFactory combinerFactory = aggregation.getCombinerFactory();
            ReducerFactory reducerFactory = aggregation.getReducerFactory();
            Collator collator = aggregation.getCollator();

            MappingJob mappingJob = job.mapper(mapper);
            ReducingSubmittableJob reducingJob;
            if (combinerFactory == null) {
                reducingJob = mappingJob.reducer(reducerFactory);
            } else {
                reducingJob = mappingJob.combiner(combinerFactory).reducer(reducerFactory);
            }

            ICompletableFuture<Result> future = reducingJob.submit(collator);
            return future.get();
        } catch (Exception e) {
            //todo: not what we want because it can lead to wrapping of even hazelcastexception
            throw new HazelcastException(e);
        }
    }

    protected Object invoke(Operation operation, int partitionId) throws Throwable {
        NodeEngine nodeEngine = getNodeEngine();
        Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
        Object response = f.get();
        Object returnObj = toObject(response);
        if (returnObj instanceof Throwable) {
            throw (Throwable) returnObj;
        }
        return returnObj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IMap");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

