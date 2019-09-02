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

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.multimap.impl.operations.EntrySetResponse;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.SetUtil.createHashSet;

@SuppressWarnings("checkstyle:methodcount")
public class ObjectMultiMapProxy<K, V>
        extends MultiMapProxySupport
        implements MultiMap<K, V>, InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    public ObjectMultiMapProxy(MultiMapConfig config, MultiMapService service, NodeEngine nodeEngine, String name) {
        super(config, service, nodeEngine, name);
    }

    @Override
    public void initialize() {
        NodeEngine nodeEngine = getNodeEngine();
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

    @Override
    public boolean put(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return putInternal(dataKey, dataValue, -1);
    }

    @Nonnull
    @Override
    public Collection<V> get(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        MultiMapResponse result = getAllInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    @Override
    public boolean remove(@Nonnull Object key, @Nonnull Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return removeInternal(dataKey, dataValue);
    }

    @Nonnull
    @Override
    public Collection<V> remove(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        MultiMapResponse result = removeInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public void delete(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        deleteInternal(dataKey);
    }

    @Nonnull
    @Override
    public Set<K> localKeySet() {
        ensureNoSplitBrain(SplitBrainProtectionOn.READ);
        Set<Data> dataKeySet = localKeySetInternal();
        return toObjectSet(dataKeySet);
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        Set<Data> dataKeySet = keySetInternal();
        return toObjectSet(dataKeySet);
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        NodeEngine nodeEngine = getNodeEngine();
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

    @Nonnull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        NodeEngine nodeEngine = getNodeEngine();
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

    @Override
    public boolean containsKey(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return containsInternal(dataKey, null);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(null, valueKey);
    }

    @Override
    public boolean containsEntry(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(dataKey, valueKey);
    }

    @Override
    public int valueCount(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return countInternal(dataKey);
    }

    @Nonnull
    @Override
    public String addLocalEntryListener(@Nonnull EntryListener<K, V> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        return getService().addListener(name, listener, null, false, true);
    }

    @Nonnull
    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        return getService().addListener(name, listener, null, includeValue, false);
    }

    @Override
    public boolean removeEntryListener(@Nonnull String registrationId) {
        checkNotNull(registrationId, "Registration ID should not be null!");
        return getService().removeListener(name, registrationId);
    }

    @Nonnull
    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return getService().addListener(name, listener, dataKey, includeValue, false);
    }

    @Override
    public void lock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.lock(nodeEngine, dataKey);
    }

    @Override
    public void lock(@Nonnull K key, long leaseTime, @Nonnull TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeUnit, "Null timeUnit is not allowed!");
        checkPositive(leaseTime, "leaseTime should be positive");

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.lock(nodeEngine, dataKey, timeUnit.toMillis(leaseTime));
    }

    @Override
    public boolean isLocked(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.isLocked(nodeEngine, dataKey);
    }

    @Override
    public boolean tryLock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.tryLock(nodeEngine, dataKey);
    }

    @Override
    public boolean tryLock(@Nonnull K key, long time, TimeUnit timeunit)
            throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.tryLock(nodeEngine, dataKey, time, timeunit);
    }

    @Override
    public boolean tryLock(@Nonnull K key,
                           long time, @Nullable TimeUnit timeunit,
                           long leaseTime, @Nullable TimeUnit leaseUnit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.tryLock(nodeEngine, dataKey, time, timeunit, leaseTime, leaseUnit);
    }

    @Override
    public void unlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.unlock(nodeEngine, dataKey);
    }

    @Override
    public void forceUnlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.forceUnlock(nodeEngine, dataKey);
    }

    @Nonnull
    @Override
    public LocalMultiMapStats getLocalMultiMapStats() {
        return getService().createStats(name);
    }

    private Set<K> toObjectSet(Set<Data> dataSet) {
        NodeEngine nodeEngine = getNodeEngine();
        Set<K> keySet = createHashSet(dataSet.size());
        for (Data dataKey : dataSet) {
            keySet.add((K) nodeEngine.toObject(dataKey));
        }
        return keySet;
    }

    private void ensureNoSplitBrain(SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        getService().ensureNoSplitBrain(name, requiredSplitBrainProtectionPermissionType);
    }
}
