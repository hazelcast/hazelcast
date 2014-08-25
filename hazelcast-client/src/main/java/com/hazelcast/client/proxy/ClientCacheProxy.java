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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.client.CacheContainsKeyRequest;
import com.hazelcast.cache.client.CacheGetAllRequest;
import com.hazelcast.cache.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.client.CacheGetRequest;
import com.hazelcast.cache.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.client.CachePutRequest;
import com.hazelcast.cache.client.CacheRemoveRequest;
import com.hazelcast.cache.client.CacheReplaceRequest;
import com.hazelcast.cache.client.CacheSizeRequest;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.nearcache.IClientNearCache;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.client.MapGetAllRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientCacheProxy<K, V>  implements ICache<K, V> {
//WARNING:: this proxy do not extend ClientProxy because Cache and DistributedObject
// has getName method which have different values a distributedObject delegate used to over come this

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private final CacheConfig<K, V> cacheConfig;

    //this will represent the name from the user perspective
    private String name;

    private boolean isClosed = false;
    protected ClientCacheDistributedObject delegate;
    private HazelcastClientCacheManager cacheManager;

//    private CacheLoader<K, V> cacheLoader;

    private volatile IClientNearCache<Data, Object> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();
    private final boolean cacheOnUpdate;

    public ClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientCacheDistributedObject delegate, HazelcastClientCacheManager cacheManager) {
        this.name = cacheConfig.getName();
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        this.cacheManager = cacheManager;
        //TODO DO WE NEED A CACHE LOADER HERE
//        if (cacheConfig.getCacheLoaderFactory() != null) {
//            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
//            cacheLoader = cacheLoaderFactory.create();
//        }

        NearCacheConfig config = delegate.getClientContext().getClientConfig().getNearCacheConfig(name);
        if (config != null) {
            nearCache = new ClientHeapNearCache<Data>(name, delegate.getClientContext(), config);
            cacheOnUpdate = config.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
        } else {
            nearCache = null;
            cacheOnUpdate = false;
        }
    }

    //region JAVAX.CACHE impl
    @Override
    public V get(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return (V) cached;
        }
        final CacheGetRequest request = new CacheGetRequest(delegate.getName(), keyData);
        try {
            final Data resultData = (Data) invoke(request, keyData);
            final V result = toObject(resultData);
            storeInNearCache(keyData, resultData, result);
            return result;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        ensureOpen();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Set<Data> keySet = new HashSet(keys.size());
        Map<K, V> result = new HashMap<K, V>();
        for (Object key : keys) {
            keySet.add(toData(key));
        }
        if (keySet.isEmpty()) {
            return result;
        }
        if (nearCache != null) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        CacheGetAllRequest request = new CacheGetAllRequest(getDistributedObjectName(), keySet);
        MapEntrySet mapEntrySet = invoke(request);
        Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Map.Entry<Data, Data> dataEntry : entrySet) {
            final Data valueData = dataEntry.getValue();
            final Data keyData = dataEntry.getKey();
            final K key = toObject(keyData);
            final V value = toObject(valueData);
            result.put(key, value);
            storeInNearCache(keyData, valueData, value);
        }
        return result;
    }

    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return true;
        }
        CacheContainsKeyRequest request = new CacheContainsKeyRequest(getDistributedObjectName(), keyData);
        return toObject( invoke(request, keyData) );
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException("loadAll");
    }

    @Override
    public void put(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = delegate.toData(key);
        final Data valueData = delegate.toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData);
        invoke(request, keyData);
        if (cacheOnUpdate) {
            storeInNearCache(keyData, valueData, value);
        } else {
            invalidateNearCache(keyData);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, null, true);
        final Object oldValue = invoke(request, keyData);
        if (cacheOnUpdate) {
            storeInNearCache(keyData, valueData, value);
        } else {
            invalidateNearCache(keyData);
        }
        return toObject(oldValue);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException("putAll");
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(getDistributedObjectName(), keyData, valueData, null);
        Boolean put = toObject(invoke(request, keyData));
        if (put == null) {
            return false;
        }
        if (put) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return put;    }

    @Override
    public boolean remove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Data keyData = delegate.toData(key);
        CacheRemoveRequest request = new CacheRemoveRequest(delegate.getName(), keyData);
        Boolean removed = toObject(invoke(request, keyData));
        if (removed == null) {
            return false;
        }
        if (removed) {
            invalidateNearCache(keyData);
        }
        return removed;
    }

    @Override
    public boolean remove(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheRemoveRequest request = new CacheRemoveRequest(name, keyData, valueData);
        Boolean removed = toObject(invoke(request, keyData));
        if (removed == null) {
            return false;
        }
        if (removed) {
            invalidateNearCache(keyData);
        }
        return removed;
    }

    @Override
    public V getAndRemove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Data keyData = toData(key);
        CacheGetAndRemoveRequest request = new CacheGetAndRemoveRequest(name, keyData);
        V value = toObject(invoke(request, keyData));
        invalidateNearCache(keyData);
        return value;
    }

    @Override
    public boolean replace(K key, V currentValue, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Data currentValueData = toData(currentValue);
        final Data valueData = toData(value);
        CacheReplaceRequest request = new CacheReplaceRequest(name, keyData, currentValueData, valueData, null);
        Boolean replaced = toObject(invoke(request, keyData));
        if (replaced == null) {
            return false;
        }
        if (replaced) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return replaced;    }

    @Override
    public boolean replace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheReplaceRequest request = new CacheReplaceRequest(name, keyData, valueData, null);
        Boolean replaced = toObject(invoke(request, keyData));
        if (replaced == null) {
            return false;
        }
        if (replaced) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return replaced;    }

    @Override
    public V getAndReplace(K key, V value) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheGetAndReplaceRequest request = new CacheGetAndReplaceRequest(name, keyData, valueData);
        V currentValue = toObject(invoke(request, keyData));
        if (currentValue != null) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return currentValue;    }

    @Override
    public void removeAll(Set<? extends K> keys) {

    }

    @Override
    public void removeAll() {

    }

    @Override
    public void clear() {

    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig);
        }
        throw new IllegalArgumentException("The configuration class " + clazz +
                " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        //TODO CHECK this is valid
/*
        must close and release all resources being coordinated on behalf of the Cache by the
        CacheManager. This includes calling the close method on configured CacheLoader,
                CacheWriter, registered CacheEntryListeners and ExpiryPolicy instances that
        implement the java.io.Closeable interface,
*/
        isClosed = true;
        delegate.destroy();

        //close the configured CacheLoader
//        if (cacheLoader instanceof Closeable) {
//            try {
//                ((Closeable) cacheLoader).close();
//            } catch (IOException e) {
//                //log
//            }
//        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return null;
    }
    //endregion



    void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    //region ICACHE imple
    @Override
    public Future<V> getAsync(K key) {
        return null;
    }

    @Override
    public Future<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (shouldBeSync()) {
            V value = get(key);
            return createCompletedFuture(value);
        }
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return createCompletedFuture(cached);
        }

        CacheGetRequest request = new CacheGetRequest(name, keyData);
        ClientCallFuture future;
        final ClientContext context = getContext();
        try {
            future = (ClientCallFuture) context.getInvocationService().invokeOnKeyOwner(request, keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (nearCache != null) {
            future.andThenInternal(new ExecutionCallback<Data>() {
                public void onResponse(Data valueData) {
                    storeInNearCache(keyData, valueData, null);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Void> putAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }

        if (shouldBeSync()) {
            put(key, value, expiryPolicy);
            return createCompletedFuture(null);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(name, keyData, valueData, expiryPolicy, false);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Void>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (shouldBeSync()) {
            V oldValue = getAndPut(key, value, expiryPolicy);
            return createCompletedFuture(oldValue);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(name, keyData, valueData, expiryPolicy, true);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> removeAsync(K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (shouldBeSync()) {
            remove(key);
            return createCompletedFuture(null);
        }
        final Data keyData = toData(key);
        CacheRemoveRequest request = new CacheRemoveRequest(name, keyData);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> removeAsync(K key, V oldValue) {
        return null;
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        return null;
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return null;
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value) {
        return null;
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {

    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {

    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return false;
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        return null;
    }

    @Override
    public int size() {
        CacheSizeRequest request = new CacheSizeRequest(name);
        Integer result = invoke(request);
        if (result == null) {
            return 0;
        }
        return result;
    }
    //endregion

    private Future createCompletedFuture(Object value) {
        return new CompletedFuture(getContext().getSerializationService(), value,
                getContext().getExecutionService().getAsyncExecutor());
    }

    private void validateConfiguredTypes(boolean validateValues, K key, V... values)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        final Class valueType = cacheConfig.getValueType();
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
        if (validateValues) {
            for (V value : values) {
                if (Object.class != valueType) {
                    //means type checks required
                    if (!valueType.isAssignableFrom(value.getClass())) {
                        throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
                    }
                }
            }
        }
    }

    private ClientContext getContext() {
        return delegate.getClientContext();
    }

    private void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null ) {
            final Object valueToStore;
            if(nearCache.getInMemoryFormat() == InMemoryFormat.OBJECT ){
                valueToStore = value != null ? value : valueData;
            } else {
                valueToStore = valueData != null ? valueData : value;
            }
            nearCache.put(key, valueToStore);
        }
    }

    private void invalidateNearCache(Data key) {
        if (nearCache != null) {
            nearCache.remove(key);
        }
    }

    private boolean shouldBeSync() {
        boolean sync = false;
//TODO Implement a backpressure stuff here
        return sync;
    }

    private String getDistributedObjectName(){
        return delegate.getName();
    }

    private <T> T toObject(Object data) {
        return delegate.toObject(data);
    }

    private Data toData(Object o) {
        return delegate.toData(o);
    }

    private <T> T invoke(ClientRequest req, Address address) {
        return delegate.invoke(req, address);
    }

    private <T> T invoke(ClientRequest req) {
        return delegate.invoke(req);
    }

    private <T> T invokeInterruptibly(ClientRequest req, Object key)
            throws InterruptedException {
        return delegate.invokeInterruptibly(req, key);
    }

    private Object invoke(ClientRequest req, Object key) {
        try {
            final Future future = delegate.getClientContext().getInvocationService().invokeOnKeyOwner(req, key);
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
