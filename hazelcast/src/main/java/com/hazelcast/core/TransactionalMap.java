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

import com.hazelcast.query.Predicate;
import com.hazelcast.transaction.TransactionalObject;

import java.util.Collection;
import java.util.Set;

/**
 * Transactional implementation of {@link BaseMap}.
 *
 * @see BaseMap
 * @see IMap
 * @param <K> key
 * @param <V> value
 */
public interface TransactionalMap<K, V> extends TransactionalObject, BaseMap<K, V> {

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#containsKey(Object)}.
     *
     * @see IMap#containsKey(Object)
     */
    boolean containsKey(Object key);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#get(Object)}.
     *
     * @see IMap#get(Object)
     */
    V get(Object key);

    /**
     * Locks the key and then gets and returns the value to which the specified key is mapped. Lock will be released at the end of the transaction (either commit or rollback).
     *
     * @see IMap#get(Object)
     */
    V getForUpdate(Object key);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#size()}.
     *
     * @see com.hazelcast.core.IMap#size()
     */
    int size();

    /**
     * Transactional implementation of {@link IMap#isEmpty()}.
     *
     * @see com.hazelcast.core.IMap#isEmpty()
     */
    boolean isEmpty();

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#put(Object, Object)}.
     * <p/>
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     *
     * @see IMap#put(Object, Object)
     */
    V put(K key, V value);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#set(Object, Object)}.
     * <p/>
     * The object to be set will be accessible only in the current transaction context till transaction is committed.
     *
     * @see IMap#set(Object, Object)
     */
    void set(K key, V value);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#putIfAbsent(Object, Object)}.
     * <p/>
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     *
     * @see IMap#putIfAbsent(Object, Object)
     */
    V putIfAbsent(K key, V value);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#replace(Object, Object)}.
     * <p/>
     * The object to be replaced will be accessible only in the current transaction context till transaction is committed.
     *
     * @see IMap#replace(Object, Object)
     */
    V replace(K key, V value);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#replace(Object, Object, Object)}.
     * <p/>
     * The object to be replaced will be accessible only in the current transaction context till transaction is committed.
     *
     * @see IMap#replace(Object, Object, Object)
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#remove(Object)}.
     * <p/>
     * The object to be removed will be removed from only the current transaction context till transaction is committed.
     *
     * @see IMap#remove(Object)
     */
    V remove(Object key);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#delete(Object)}.
     * <p/>
     * The object to be deleted will be removed from only the current transaction context till transaction is committed.
     *
     * @see IMap#delete(Object)
     */
    void delete(Object key);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#remove(Object, Object)}.
     * <p/>
     * The object to be removed will be removed from only the current transaction context till transaction is committed.
     *
     * @see IMap#remove(Object, Object)
     */
    boolean remove(Object key, Object value);

    /**
     * Transactional implementation of {@link IMap#keySet()}.
     * <p/>
     *
     * @see com.hazelcast.core.IMap#keySet()
     */
    Set<K> keySet();

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#keySet(com.hazelcast.query.Predicate)} .
     * <p/>
     *
     * @see IMap#keySet(com.hazelcast.query.Predicate)
     */
    Set<K> keySet(Predicate predicate);

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#values()}.
     * <p/>
     *
     * @see IMap#values()
     */
    Collection<V> values();

    /**
     * Transactional implementation of {@link com.hazelcast.core.IMap#values(com.hazelcast.query.Predicate)} .
     * <p/>
     *
     * @see IMap#values(com.hazelcast.query.Predicate)
     */
    Collection<V> values(Predicate predicate);
}
