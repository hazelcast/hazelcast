/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.monitor.LocalMapStats;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.Set;

/**
 * Abstracts the Hazelcast data structures with Near Cache support for the Near Cache usage.
 */
public interface DataStructureAdapter<K, V> {

    V get(K key);

    ICompletableFuture<V> getAsync(K key);

    void set(K key, V value);

    V put(K key, V value);

    boolean putIfAbsent(K key, V value);

    ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value);

    V replace(K key, V newValue);

    boolean replace(K key, V oldValue, V newValue);

    void remove(K key);

    boolean remove(K key, V oldValue);

    ICompletableFuture<V> removeAsync(K key);

    boolean containsKey(K key);

    Map<K, V> getAll(Set<K> keys);

    void putAll(Map<K, V> map);

    void removeAll();

    void removeAll(Set<K> keys);

    void clear();

    LocalMapStats getLocalMapStats();

    /**
     * Contains all methods of {@link DataStructureAdapter}.
     */
    enum DataStructureMethods implements DataStructureAdapterMethod {
        GET("get", Object.class),
        GET_ASYNC("getAsync", Object.class),
        SET("set", Object.class, Object.class),
        PUT("put", Object.class, Object.class),
        PUT_IF_ABSENT("putIfAbsent", Object.class, Object.class),
        PUT_IF_ABSENT_ASYNC("putIfAbsentAsync", Object.class, Object.class),
        REPLACE("replace", Object.class, Object.class),
        REPLACE_WITH_OLD_VALUE("replace", Object.class, Object.class, Object.class),
        REMOVE("remove", Object.class),
        REMOVE_WITH_OLD_VALUE("remove", Object.class, Object.class),
        REMOVE_ASYNC("removeAsync", Object.class),
        CONTAINS_KEY("containsKey", Object.class),
        GET_ALL("getAll", Set.class),
        PUT_ALL("putAll", Map.class),
        REMOVE_ALL("removeAll"),
        REMOVE_ALL_WITH_KEYS("removeAll", Set.class),
        CLEAR("clear"),
        GET_LOCAL_MAP_STATS("getLocalMapStats");

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
