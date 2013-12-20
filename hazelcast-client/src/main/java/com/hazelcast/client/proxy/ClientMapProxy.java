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

package com.hazelcast.client.proxy;

import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.nearcache.ClientNearCacheType;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.*;
import com.hazelcast.map.client.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 5/17/13
 */
public final class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    private final String name;
    private volatile ClientNearCache<Data> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

    public ClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        throw new UnsupportedOperationException("FIXME!!!!!");
    }

    @Override
    public boolean containsKey(Object key) {
        Data keyData = toData(key);
        MapContainsKeyRequest request = new MapContainsKeyRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean containsValue(Object value) {
        Data valueData = toData(value);
        MapContainsValueRequest request = new MapContainsValueRequest(name, valueData);
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public V get(Object key) {
        initNearCache();

        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null) {
                if (cached.equals(ClientNearCache.NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }
        MapGetRequest request = new MapGetRequest(name, keyData);
        final V result = invoke(request, keyData);
        if (nearCache != null) {
            nearCache.put(keyData, result);
        }
        return result;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, null);
    }

    @Override
    public V remove(Object key) {
        final Data keyData = toData(key);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public boolean remove(Object key, Object value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapRemoveIfSameRequest request = new MapRemoveIfSameRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void delete(Object key) {
        final Data keyData = toData(key);
        MapDeleteRequest request = new MapDeleteRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        MapFlushRequest request = new MapFlushRequest(name);
        invoke(request);
    }

    @Override
    public Future<V> getAsync(final K key) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return get(key);
            }
        });
        return f;
    }

    @Override
    public Future<V> putAsync(final K key, final V value) {
        return putAsync(key, value, -1, null);
    }

    @Override
    public Future<V> putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return put(key, value, ttl, timeunit);
            }
        });
        return f;
    }

    @Override
    public Future<V> removeAsync(final K key) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return remove(key);
            }
        });
        return f;
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        MapTryRemoveRequest request = new MapTryRemoveRequest(name, keyData, ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapTryPutRequest request = new MapTryPutRequest(name, keyData, valueData, ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutTransientRequest request = new MapPutTransientRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1, null);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutIfAbsentRequest request = new MapPutIfAbsentRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final Data newValueData = toData(newValue);
        MapReplaceIfSameRequest request = new MapReplaceIfSameRequest(name, keyData, oldValueData, newValueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public V replace(K key, V value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapReplaceRequest request = new MapReplaceRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapSetRequest request = new MapSetRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        final Data keyData = toData(key);
        MapIsLockedRequest request = new MapIsLockedRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, timeunit));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void unlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), false);
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), true);
        invoke(request, keyData);
    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        MapAddInterceptorRequest request = new MapAddInterceptorRequest(name, interceptor);
        return invoke(request);
    }

    @Override
    public void removeInterceptor(String id) {
        MapRemoveInterceptorRequest request = new MapRemoveInterceptorRequest(name, id);
        invoke(request);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return stopListening(id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, keyData, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, keyData, includeValue, predicate);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, boolean includeValue) {
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, null, includeValue, predicate);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, null, handler);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        final Data keyData = toData(key);
        MapGetEntryViewRequest request = new MapGetEntryViewRequest(name, keyData);
        SimpleEntryView entryView = invoke(request, keyData);
        if (entryView == null) {
            return null;
        }
        final Data value = (Data) entryView.getValue();
        entryView.setKey(key);
        entryView.setValue(toObject(value));
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        final Data keyData = toData(key);
        MapEvictRequest request = new MapEvictRequest(name, keyData, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public Set<K> keySet() {
        MapKeySetRequest request = new MapKeySetRequest(name);
        MapKeySet mapKeySet = invoke(request);
        Set<Data> keySetData = mapKeySet.getKeySet();
        Set<K> keySet = new HashSet<K>(keySetData.size());
        for (Data data : keySetData) {
            final K key = toObject(data);
            keySet.add(key);
        }
        return keySet;
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Set keySet = new HashSet(keys.size());
        for (Object key : keys) {
            keySet.add(toData(key));
        }
        MapGetAllRequest request = new MapGetAllRequest(name, keySet);
        MapEntrySet mapEntrySet = invoke(request);
        Map<K, V> result = new HashMap<K, V>();
        Set<Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Entry<Data, Data> dataEntry : entrySet) {
            result.put((K) toObject(dataEntry.getKey()), (V) toObject(dataEntry.getValue()));
        }
        return result;
    }

    @Override
    public Collection<V> values() {
        MapValuesRequest request = new MapValuesRequest(name);
        MapValueCollection mapValueCollection = invoke(request);
        Collection<Data> collectionData = mapValueCollection.getValues();
        Collection<V> collection = new ArrayList<V>(collectionData.size());
        for (Data data : collectionData) {
            final V value = toObject(data);
            collection.add(value);
        }
        return collection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        MapEntrySetRequest request = new MapEntrySetRequest(name);
        MapEntrySet result = invoke(request);
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>();
        Set<Entry<Data, Data>> entries = result.getEntrySet();
        for (Entry<Data, Data> dataEntry : entries) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            V value = toObject(valueData);
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.KEY);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                keySet(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.KEY);
        QueryResultSet result = invoke(request);
        List<K> keyList = new ArrayList<K>(result.size());
        for (Object data : result) {
            K key = toObject((Data) data);
            keyList.add(key);
        }
        if (pagingPredicate != null) {
            Collections.sort(keyList, SortingUtil.newComparator(pagingPredicate.getComparator()));
            if (keyList.size() > pagingPredicate.getPageSize()) {
                keyList = keyList.subList(0, pagingPredicate.getPageSize());
            }
            Object anchor = null;
            if (keyList.size() != 0) {
                anchor = keyList.get(keyList.size()-1);
            }
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, new AbstractMap.SimpleImmutableEntry(anchor, null));
        }
        return new HashSet<K>(keyList);
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.ENTRY);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                entrySet(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }

        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.ENTRY);
        QueryResultSet result = invoke(request);
        Set entrySet;
        if (pagingPredicate == null) {
            entrySet = new HashSet<Entry<K, V>>(result.size());
        } else {
            entrySet = new SortedQueryResultSet(pagingPredicate.getComparator(), IterationType.ENTRY, pagingPredicate.getPageSize());
        }
        for (Object data : result) {
            AbstractMap.SimpleImmutableEntry<Data, Data> dataEntry = (AbstractMap.SimpleImmutableEntry<Data, Data>) data;
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        if (pagingPredicate != null) {
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) entrySet).last());
        }
        return entrySet;
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.VALUE);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                values(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.VALUE);
        QueryResultSet result = invoke(request);
        List<V> values = new ArrayList<V>(result.size());
        for (Object data : result) {
            V value = toObject((Data) data);
            values.add(value);
        }
        if (pagingPredicate != null) {
            Collections.sort(values, SortingUtil.newComparator(pagingPredicate.getComparator()));
            if (values.size() > pagingPredicate.getPageSize()) {
                values = values.subList(0, pagingPredicate.getPageSize());
            }
            Object anchor = null;
            if (values.size() != 0) {
                anchor = values.get(values.size()-1);
            }
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, new AbstractMap.SimpleImmutableEntry(null, anchor));
        }
        return values;
    }

    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public Set<K> localKeySet(Predicate predicate) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {
        MapAddIndexRequest request = new MapAddIndexRequest(name, attribute, ordered);
        invoke(request);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        final Data keyData = toData(key);
        MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        return invoke(request, keyData);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);

        getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                try {
                    V result = invoke(request, keyData);
                    callback.onResponse(result);
                    return result;
                } catch (Exception e) {
                    callback.onFailure(e);
                    throw (e);
                }
            }
        });
    }

    @Override
    public Future submitToKey(K key, EntryProcessor entryProcessor) {
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);

        return getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return invoke(request, keyData);
            }
        });
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        MapExecuteOnAllKeysRequest request = new MapExecuteOnAllKeysRequest(name, entryProcessor);
        MapEntrySet entrySet = invoke(request);
        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> dataEntry : entrySet.getEntrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        MapExecuteWithPredicateRequest request = new MapExecuteWithPredicateRequest(name, entryProcessor, predicate);
        MapEntrySet entrySet = invoke(request);
        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> dataEntry : entrySet.getEntrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public void set(K key, V value) {
        set(key, value, -1, null);
    }

    @Override
    public int size() {
        MapSizeRequest request = new MapSizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        MapEntrySet entrySet = new MapEntrySet();
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            entrySet.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(toData(entry.getKey()), toData(entry.getValue())));
        }
        MapPutAllRequest request = new MapPutAllRequest(name, entrySet);
        invoke(request);
    }

    @Override
    public void clear() {
        MapClearRequest request = new MapClearRequest(name);
        invoke(request);
    }

    @Override
    protected void onDestroy() {
        if (nearCache != null) {
            nearCache.destroy();
        }
    }

    private Data toData(Object o) {
        return getContext().getSerializationService().toData(o);
    }

    private <T> T toObject(Data data) {
        return (T) getContext().getSerializationService().toObject(data);
    }

    private <T> T invoke(Object req, Data keyData) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnRandomTarget(req);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<PortableEntryEvent> createHandler(final EntryListener<K, V> listener, final boolean includeValue) {
        return new EventHandler<PortableEntryEvent>() {
            public void handle(PortableEntryEvent event) {
                V value = null;
                V oldValue = null;
                if (includeValue) {
                    value = toObject(event.getValue());
                    oldValue = toObject(event.getOldValue());
                }
                K key = toObject(event.getKey());
                Member member = getContext().getClusterService().getMember(event.getUuid());
                EntryEvent<K, V> entryEvent = new EntryEvent<K, V>(name, member,
                        event.getEventType().getType(), key, oldValue, value);
                switch (event.getEventType()) {
                    case ADDED:
                        listener.entryAdded(entryEvent);
                        break;
                    case REMOVED:
                        listener.entryRemoved(entryEvent);
                        break;
                    case UPDATED:
                        listener.entryUpdated(entryEvent);
                        break;
                    case EVICTED:
                        listener.entryEvicted(entryEvent);
                        break;
                }
            }
        };
    }

    private void initNearCache() {
        if (nearCacheInitialized.compareAndSet(false, true)) {
            final NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
            if (nearCacheConfig == null) {
                return;
            }
            ClientNearCache<Data> _nearCache = new ClientNearCache<Data>(
                    name, ClientNearCacheType.Map, getContext(), nearCacheConfig);
            nearCache = _nearCache;
        }
    }

}
