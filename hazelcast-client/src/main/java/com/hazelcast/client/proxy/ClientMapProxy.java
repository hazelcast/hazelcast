package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.client.MapGetRequest;
import com.hazelcast.map.client.MapPutRequest;
import com.hazelcast.map.client.MapRemoveRequest;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
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

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        final Data dataKey = getSerializationService().toData(key);
        MapGetRequest req = new MapGetRequest(name, dataKey);
        try {
            return getInvocationService().invokeOnKeyOwner(req, dataKey);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public V put(K key, V value) {
        final SerializationService ss = getSerializationService();
        final Data dataKey = ss.toData(key);
        MapPutRequest req = new MapPutRequest(name, dataKey, ss.toData(value), ThreadUtil.getThreadId());
        try {
            return getInvocationService().invokeOnKeyOwner(req, dataKey);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public V remove(Object key) {
        final Data dataKey = getSerializationService().toData(key);
        MapRemoveRequest req = new MapRemoveRequest(name, dataKey, ThreadUtil.getThreadId());
        try {
            return getInvocationService().invokeOnKeyOwner(req, dataKey);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public void delete(Object key) {

    }

    @Override
    public void flush() {

    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return null;
    }

    @Override
    public Future<V> getAsync(K key) {
        return null;
    }

    @Override
    public Future<V> putAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<V> removeAsync(K key) {
        return null;
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        return false;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        return false;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        return null;
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {

    }

    @Override
    public V putIfAbsent(K key, V value) {
        return null;
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public V replace(K key, V value) {
        return null;
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {

    }

    @Override
    public void lock(K key) {

    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {

    }

    @Override
    public boolean isLocked(K key) {
        return false;
    }

    @Override
    public boolean tryLock(K key) {
        return false;
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock(K key) {

    }

    @Override
    public void forceUnlock(K key) {

    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener) {
        return null;
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        return null;
    }

    @Override
    public void removeInterceptor(String id) {

    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        return null;
    }

    @Override
    public boolean removeEntryListener(String id) {
        return false;
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        return null;
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return null;
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        return null;
    }

    @Override
    public boolean evict(K key) {
        return false;
    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        return null;
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        return null;
    }

    @Override
    public Set<K> localKeySet() {
        return null;
    }

    @Override
    public Set<K> localKeySet(Predicate predicate) {
        return null;
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {

    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return null;
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        return null;
    }

    @Override
    public Map<K, Object> executeOnAllKeys(EntryProcessor entryProcessor) {
        return null;
    }

    @Override
    public void set(K key, V value) {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    protected void onDestroy() {
    }

    public String getName() {
        return name;
    }
}
