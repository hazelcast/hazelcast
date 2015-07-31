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

package com.hazelcast.multimap.impl;

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
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
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.multimap.impl.operations.EntrySetResponse;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.isNotNull;

public class ObjectMultiMapProxy<K, V>
        extends MultiMapProxySupport
        implements MultiMap<K, V>, InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    public ObjectMultiMapProxy(MultiMapService service, NodeEngine nodeEngine, String name) {
        super(service, nodeEngine, name);
    }

    public String getName() {
        return name;
    }

    @Override
    public void initialize() {
        final NodeEngine nodeEngine = getNodeEngine();
        List<EntryListenerConfig> listenerConfigs = config.getEntryListenerConfigs();
        for (EntryListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());
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
                    addEntryListener(listener, listenerConfig.isIncludeValue());
                }
            }
        }
    }

    public boolean put(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return putInternal(dataKey, dataValue, -1);
    }

    public Collection<V> get(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        MultiMapResponse result = getAllInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return removeInternal(dataKey, dataValue);
    }

    public Collection<V> remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        MultiMapResponse result = removeInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public Set<K> localKeySet() {
        Set<Data> dataKeySet = localKeySetInternal();
        return toObjectSet(dataKeySet);
    }

    public Set<K> keySet() {
        Set<Data> dataKeySet = keySetInternal();
        return toObjectSet(dataKeySet);
    }

    public Collection<V> values() {
        final NodeEngine nodeEngine = getNodeEngine();
        Map map = valuesInternal();
        Collection values = new LinkedList();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            MultiMapResponse response = nodeEngine.toObject(obj);
            values.addAll(response.getObjectCollection(nodeEngine));
        }
        return values;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        final NodeEngine nodeEngine = getNodeEngine();
        Map map = entrySetInternal();
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            EntrySetResponse response = nodeEngine.toObject(obj);
            Set<Map.Entry<K, V>> entries = response.getObjectEntrySet(nodeEngine);
            entrySet.addAll(entries);
        }
        return entrySet;
    }

    public boolean containsKey(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return containsInternal(dataKey, null);
    }

    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(null, valueKey);
    }

    public boolean containsEntry(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(dataKey, valueKey);
    }

    public int valueCount(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return countInternal(dataKey);
    }

    public String addLocalEntryListener(EntryListener<K, V> listener) {
        return getService().addListener(name, listener, null, false, true);
    }

    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        return getService().addListener(name, listener, null, includeValue, false);
    }

    public boolean removeEntryListener(String registrationId) {
        return getService().removeListener(name, registrationId);
    }

    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return getService().addListener(name, listener, dataKey, includeValue, false);
    }

    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.lock(nodeEngine, dataKey);
    }

    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive(leaseTime, "leaseTime should be positive");

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.lock(nodeEngine, dataKey, timeUnit.toMillis(leaseTime));
    }

    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.isLocked(nodeEngine, dataKey);
    }

    public boolean tryLock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        try {
            return tryLock(key, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit)
            throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.tryLock(nodeEngine, dataKey, time, timeunit);
    }

    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.unlock(nodeEngine, dataKey);
    }

    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.forceUnlock(nodeEngine, dataKey);
    }

    public LocalMultiMapStats getLocalMultiMapStats() {
        return (LocalMultiMapStats) getService().createStats(name);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {


        HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-multimap-" + getName());
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {

        try {
            isNotNull(jobTracker, "jobTracker");
            KeyValueSource<K, V> keyValueSource = KeyValueSource.fromMultiMap(this);
            Job<K, V> job = jobTracker.newJob(keyValueSource);
            Mapper mapper = aggregation.getMapper(supplier);
            CombinerFactory combinerFactory = aggregation.getCombinerFactory();
            ReducerFactory reducerFactory = aggregation.getReducerFactory();
            Collator collator = aggregation.getCollator();

            MappingJob mappingJob = job.mapper(mapper);
            ReducingSubmittableJob reducingJob;
            if (combinerFactory != null) {
                reducingJob = mappingJob.combiner(combinerFactory).reducer(reducerFactory);
            } else {
                reducingJob = mappingJob.reducer(reducerFactory);
            }

            ICompletableFuture<Result> future = reducingJob.submit(collator);
            return future.get();
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    private Set<K> toObjectSet(Set<Data> dataSet) {
        final NodeEngine nodeEngine = getNodeEngine();
        Set<K> keySet = new HashSet<K>(dataSet.size());
        for (Data dataKey : dataSet) {
            keySet.add((K) nodeEngine.toObject(dataKey));
        }
        return keySet;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MultiMap{");
        sb.append("name=").append(getName());
        sb.append('}');
        return sb.toString();
    }
}
