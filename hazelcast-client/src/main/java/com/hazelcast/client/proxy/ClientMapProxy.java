package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.map.*;
import com.hazelcast.map.client.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryDataResultStream;
import com.hazelcast.util.ThreadUtil;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 5/17/13
 */
public final class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    private final String name;

    public ClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    public boolean containsKey(Object key) {
        Data keyData = toData(key);
        MapContainsKeyRequest request = new MapContainsKeyRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean containsValue(Object value) {
        Data valueData = toData(value);
        MapContainsValueRequest request = new MapContainsValueRequest(name, valueData);
        Boolean result = invoke(request);
        return result;
    }

    public V get(Object key) {
        final Data keyData = toData(key);
        MapGetRequest request = new MapGetRequest(name, keyData);
        return invoke(request, keyData);
    }

    public V put(K key, V value) {
        return put(key, value, -1, null);
    }

    public V remove(Object key) {
        final Data keyData = toData(key);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    public boolean remove(Object key, Object value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapRemoveIfSameRequest request = new MapRemoveIfSameRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public void delete(Object key) {
        final Data keyData = toData(key);
        MapDeleteRequest request = new MapDeleteRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    public void flush() {
        MapFlushRequest request = new MapFlushRequest(name);
        invoke(request);
    }

    public Map<K, V> getAll(Set<K> keys) {
        MapGetAllRequest request = new MapGetAllRequest(name);
        invoke(request);
        //TODO complete
        return null;
    }

    public Future<V> getAsync(final K key) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return get(key);
            }
        });
        return f;
    }

    public Future<V> putAsync(final K key, final V value) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return put(key, value);
            }
        });
        return f;
    }

    public Future<V> removeAsync(final K key) {
        Future<V> f = getContext().getExecutionService().submit(new Callable<V>() {
            public V call() throws Exception {
                return remove(key);
            }
        });
        return f;
    }

    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        MapTryRemoveRequest request = new MapTryRemoveRequest(name, keyData, ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapTryPutRequest request = new MapTryPutRequest(name, keyData, valueData, ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutTransientRequest request = new MapPutTransientRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request);
    }

    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1, null);
    }

    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapPutIfAbsentRequest request = new MapPutIfAbsentRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    public boolean replace(K key, V oldValue, V newValue) {
        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final Data newValueData = toData(newValue);
        MapReplaceIfSameRequest request = new MapReplaceIfSameRequest(name, keyData, oldValueData, newValueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public V replace(K key, V value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapReplaceRequest request = new MapReplaceRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        MapSetRequest request = new MapSetRequest(name, keyData, valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
    }

    public void lock(K key) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request, keyData);
    }

    public boolean isLocked(K key) {
        final Data keyData = toData(key);
        MapIsLockedRequest request = new MapIsLockedRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, timeunit));
        Boolean result = invoke(request, keyData);
        return result;
    }

    public void unlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), false);
        invoke(request, keyData);
    }

    public void forceUnlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), true);
        invoke(request, keyData);
    }

    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public String addInterceptor(MapInterceptor interceptor) {
        MapAddInterceptorRequest request = new MapAddInterceptorRequest(name, interceptor);
        return invoke(request);
    }

    public void removeInterceptor(String id) {
        MapRemoveInterceptorRequest request = new MapRemoveInterceptorRequest(name, id);
        invoke(request);
    }

    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        return null;
    }

    public boolean removeEntryListener(String id) {
        return false;
    }

    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        return null;
    }

    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return null;
    }

    public EntryView<K, V> getEntryView(K key) {
        final Data keyData = toData(key);
        MapGetEntryViewRequest request = new MapGetEntryViewRequest(name, keyData);
        SimpleEntryView entryView = invoke(request, keyData);
        if(entryView == null) {
            return null;
        }
        final Data value = (Data)entryView.getValue();
        entryView.setKey(key);
        entryView.setValue(toObject(value));
        return entryView;
    }

    public boolean evict(K key) {
        final Data keyData = toData(key);
        MapEvictRequest request = new MapEvictRequest(name, keyData, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

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

    public Set<Entry<K, V>> entrySet() {
        MapEntrySetRequest request = new MapEntrySetRequest(name);
        Set<Entry<Data, Data>> result = invoke(request); //TODO result may change
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>(result.size());
        for (Entry<Data, Data> dataEntry : result) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            V value = toObject(valueData);
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    public Set<K> keySet(Predicate predicate) {
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.KEY);
        QueryDataResultStream result = invoke(request);
        Set<K> keySet = new HashSet<K>(result.size());
        for (QueryResultEntry queryResultEntry : result) {
            Data keyData = queryResultEntry.getKeyData();
            K key = toObject(keyData);
            keySet.add(key);
        }
        return keySet;
    }

    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.ENTRY);
        QueryDataResultStream result = invoke(request);
        Set<Entry<K,V>> entrySet = new HashSet<Entry<K, V>>(result.size());
        for (QueryResultEntry queryResultEntry : result) {
            Data keyData = queryResultEntry.getKeyData();
            Data valueData = queryResultEntry.getValueData();
            K key = toObject(keyData);
            V value = toObject(valueData);
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    public Collection<V> values(Predicate predicate) {
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.VALUE);
        QueryDataResultStream result = invoke(request);
        Collection<V> values = new ArrayList<V>(result.size());
        for (QueryResultEntry queryResultEntry : result) {
            Data valueData = queryResultEntry.getValueData();
            V value = toObject(valueData);
            values.add(value);
        }
        return values;
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public Set<K> localKeySet(Predicate predicate) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public void addIndex(String attribute, boolean ordered) {
        MapAddIndexRequest request = new MapAddIndexRequest(name, attribute, ordered);
        invoke(request);
    }

    public LocalMapStats getLocalMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        final Data keyData = toData(key);
        MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        return invoke(request, keyData);
    }

    public Map<K, Object> executeOnAllKeys(EntryProcessor entryProcessor) {
        MapExecuteOnAllKeysRequest request = new MapExecuteOnAllKeysRequest(name, entryProcessor);
        Map<Data, Data> result = invoke(request);//TODO result may change
        Map<K, Object> map = new HashMap<K, Object>(result.size());
        for (Entry<Data, Data> dataEntry : result.entrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            map.put(key, toObject(valueData));
        }
        return map;
    }

    public void set(K key, V value) {
        set(key, value, -1, null);
    }

    public int size() {
        MapSizeRequest request = new MapSizeRequest(name);
        Integer result =invoke(request);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        //TODO no request?
    }

    public void clear() {
        //TODO no request?
    }

    protected void onDestroy() {
        //TODO ?
    }

    public String getName() {
        return name;
    }

    private Data toData(Object o){
        return getContext().getSerializationService().toData(o);
    }

    private <T> T toObject(Data data){
        return (T) getContext().getSerializationService().toObject(data);
    }

    private <T> T invoke(Object req, Data keyData){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnRandomTarget(req);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

}
