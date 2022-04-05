/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.adapter;

import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Abstracts the Hazelcast data structures with Near Cache support for the Near Cache usage.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DataStructureAdapter<K, V> {

    int size();

    V get(K key);

    CompletionStage<V> getAsync(K key);

    void set(K key, V value);

    CompletionStage<Void> setAsync(K key, V value);

    CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit);

    CompletionStage<Void> setAsync(K key, V value, ExpiryPolicy expiryPolicy);

    V put(K key, V value);

    CompletionStage<V> putAsync(K key, V value);

    CompletionStage<V> putAsync(K key, V value, long ttl, TimeUnit timeunit);

    CompletionStage<V> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    void putTransient(K key, V value, long ttl, TimeUnit timeunit);

    boolean putIfAbsent(K key, V value);

    CompletionStage<Boolean> putIfAbsentAsync(K key, V value);

    void setTtl(K key, long duration, TimeUnit timeUnit);

    V replace(K key, V newValue);

    boolean replace(K key, V oldValue, V newValue);

    @MethodNotAvailable
    default V getAndReplace(K key, V value) {
        throw new MethodNotAvailableException();
    }

    @MethodNotAvailable
    default CompletionStage<V> getAndReplaceAsync(K key, V value) {
        throw new MethodNotAvailableException();
    }

    V remove(K key);

    boolean remove(K key, V oldValue);

    CompletionStage<V> removeAsync(K key);

    @MethodNotAvailable
    default V getAndRemove(K key) {
        throw new MethodNotAvailableException();
    }

    @MethodNotAvailable
    default CompletionStage<V> getAndRemoveAsync(K key) {
        throw new MethodNotAvailableException();
    }

    void delete(K key);

    CompletionStage<Boolean> deleteAsync(K key);

    boolean evict(K key);

    <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException;

    Object executeOnKey(K key, com.hazelcast.map.EntryProcessor entryProcessor);

    Map<K, Object> executeOnKeys(Set<K> keys, com.hazelcast.map.EntryProcessor entryProcessor);

    Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor);

    Map<K, Object> executeOnEntries(com.hazelcast.map.EntryProcessor entryProcessor, Predicate predicate);

    boolean containsKey(K key);

    void loadAll(boolean replaceExistingValues);

    void loadAll(Set<K> keys, boolean replaceExistingValues);

    void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener);

    Map<K, V> getAll(Set<K> keys);

    void putAll(Map<K, V> map);

    void removeAll();

    void removeAll(Set<K> keys);

    void evictAll();

    <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                  Object... arguments);

    void clear();

    void close();

    void destroy();

    void setExpiryPolicy(Set<K> keys, ExpiryPolicy expiryPolicy);

    boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy);

    LocalMapStats getLocalMapStats();

    /**
     * Contains all methods of {@link DataStructureAdapter}.
     */
    enum DataStructureMethods implements DataStructureAdapterMethod {
        SIZE("size"),
        GET("get", Object.class),
        GET_ASYNC("getAsync", Object.class),
        SET("set", Object.class, Object.class),
        SET_ASYNC("setAsync", Object.class, Object.class),
        SET_ASYNC_WITH_TTL("setAsync", Object.class, Object.class, long.class, TimeUnit.class),
        SET_ASYNC_WITH_EXPIRY_POLICY("setAsync", Object.class, Object.class, ExpiryPolicy.class),
        PUT("put", Object.class, Object.class),
        PUT_ASYNC("putAsync", Object.class, Object.class),
        PUT_ASYNC_WITH_TTL("putAsync", Object.class, Object.class, long.class, TimeUnit.class),
        PUT_ASYNC_WITH_EXPIRY_POLICY("putAsync", Object.class, Object.class, ExpiryPolicy.class),
        PUT_TRANSIENT("putTransient", Object.class, Object.class, long.class, TimeUnit.class),
        PUT_IF_ABSENT("putIfAbsent", Object.class, Object.class),
        PUT_IF_ABSENT_ASYNC("putIfAbsentAsync", Object.class, Object.class),
        REPLACE("replace", Object.class, Object.class),
        REPLACE_WITH_OLD_VALUE("replace", Object.class, Object.class, Object.class),
        GET_AND_REPLACE("getAndReplace", Object.class, Object.class),
        GET_AND_REPLACE_ASYNC("getAndReplaceAsync", Object.class, Object.class),
        REMOVE("remove", Object.class),
        REMOVE_WITH_OLD_VALUE("remove", Object.class, Object.class),
        REMOVE_ASYNC("removeAsync", Object.class),
        GET_AND_REMOVE("getAndRemove", Object.class),
        GET_AND_REMOVE_ASYNC("getAndRemoveAsync", Object.class),
        DELETE("delete", Object.class),
        DELETE_ASYNC("deleteAsync", Object.class),
        EVICT("evict", Object.class),
        INVOKE("invoke", Object.class, EntryProcessor.class, Object[].class),
        EXECUTE_ON_KEY("executeOnKey", Object.class, com.hazelcast.map.EntryProcessor.class),
        EXECUTE_ON_KEYS("executeOnKeys", Set.class, com.hazelcast.map.EntryProcessor.class),
        EXECUTE_ON_ENTRIES("executeOnEntries", com.hazelcast.map.EntryProcessor.class),
        EXECUTE_ON_ENTRIES_WITH_PREDICATE("executeOnEntries", com.hazelcast.map.EntryProcessor.class, Predicate.class),
        CONTAINS_KEY("containsKey", Object.class),
        LOAD_ALL("loadAll", boolean.class),
        LOAD_ALL_WITH_KEYS("loadAll", Set.class, boolean.class),
        LOAD_ALL_WITH_LISTENER("loadAll", Set.class, boolean.class, CompletionListener.class),
        GET_ALL("getAll", Set.class),
        PUT_ALL("putAll", Map.class),
        REMOVE_ALL("removeAll"),
        REMOVE_ALL_WITH_KEYS("removeAll", Set.class),
        EVICT_ALL("evictAll"),
        INVOKE_ALL("invokeAll", Set.class, EntryProcessor.class, Object[].class),
        CLEAR("clear"),
        CLOSE("close"),
        DESTROY("destroy"),
        GET_LOCAL_MAP_STATS("getLocalMapStats"),
        SET_TTL("setTtl", Object.class, long.class, TimeUnit.class),
        SET_EXPIRY_POLICY_MULTI_KEY("setExpiryPolicy", Set.class, ExpiryPolicy.class),
        SET_EXPIRY_POLICY("setExpiryPolicy", Object.class, ExpiryPolicy.class);

        private final String methodName;
        private final Class<?>[] parameterTypes;

        DataStructureMethods(String methodName, Class<?>... parameterTypes) {
            this.methodName = methodName;
            this.parameterTypes = parameterTypes;
        }

        @Override
        public String getMethodName() {
            return methodName;
        }

        @Override
        @SuppressFBWarnings("EI_EXPOSE_REP")
        public Class<?>[] getParameterTypes() {
            return parameterTypes;
        }

        @Override
        public String getParameterTypeString() {
            StringBuilder sb = new StringBuilder();
            String delimiter = "";
            for (Class<?> parameterType : parameterTypes) {
                sb.append(delimiter).append(parameterType.getSimpleName());
                delimiter = ", ";
            }
            return sb.toString();
        }
    }
}
