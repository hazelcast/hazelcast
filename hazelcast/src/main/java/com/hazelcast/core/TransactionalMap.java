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

package com.hazelcast.core;

import com.hazelcast.transaction.TransactionalObject;

public interface TransactionalMap<K, V> extends TransactionalObject, BaseMap<K, V> {

    /**
     * Transactional version of containsKey method.
     * <p>See: {@link com.hazelcast.core.IMap#containsKey(Object)}</p>
     */
    boolean containsKey(Object key);

    /**
     * Transactional version of get method.
     * <p>See: {@link com.hazelcast.core.IMap#get(Object)}</p>
     */
    V get(Object key);

    /**
     * Transactional version of size method.
     * See: {@link com.hazelcast.core.IMap#size()}
     */
    int size();

    /**
     * Transactional version of put method.
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#put(Object, Object)}</p>
     */
    V put(K key, V value);

    /**
     * Transactional version of set method.
     * The object to be set will be accessible only in the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#set(Object, Object)}</p>
     */
    void set(K key, V value);

    /**
     * Transactional version of putIfAbsent method.
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#putIfAbsent(Object, Object)}</p>
     */
    V putIfAbsent(K key, V value);

    /**
     * Transactional version of replace method.
     * The object to be replaced will be accessible only in the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#replace(Object, Object)}</p>
     */
    V replace(K key, V value);

    /**
     * Transactional version of replace method.
     * The object to be replaced will be accessible only in the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#replace(Object, Object, Object)}</p>
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * Transactional version of remove method.
     * The object to be removed will be removed from only the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#remove(Object)}</p>
     */
    V remove(Object key);

    /**
     * Transactional version of delete method.
     * The object to be deleted will be removed from only the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#delete(Object)}</p>
     */
    void delete(Object key);

    /**
     * Transactional version of remove method.
     * The object to be removed will be removed from only the current transaction context till transaction is committed.
     * <p>See: {@link com.hazelcast.core.IMap#remove(Object, Object)}</p>
     */
    boolean remove(Object key, Object value);
}
