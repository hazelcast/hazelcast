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

package com.hazelcast.core;

import com.hazelcast.query.Predicate;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.TransactionalObject;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Transactional implementation of {@link BaseMap}.
 * <p/>
 * <h2>MapStore Interaction</h2>
 * When using MapStore, the call to any MapStore method is outside the transactional boundary.
 * If you need to have an XATransaction spanning Hazelcast operations and one more other XAResources
 * (such as a database), you should not use MapStore. Instead, enlist both resources in a transaction as shown below:
 * <p/>
 * <pre>
 * <code>
 *
 * HazelcastInstance client = HazelcastClient.newHazelcastClient();
 *
 * UserTransactionManager tm = new UserTransactionManager();
 * tm.setTransactionTimeout(60);
 * tm.begin();
 *
 * HazelcastXAResource xaResource = client.getXAResource();
 * Transaction transaction = tm.getTransaction();
 * transaction.enlistResource(xaResource);
 *
 * // you can enlist more resources here like a database XAResource
 * try {
 *      TransactionContext context = xaResource.getTransactionContext()
 *      TransactionalMap map = context.getMap("map");
 *      map.put("key", "value");
 *      final TransactionalQueue queue = context.getQueue("queue");
 *      queue.offer("item");
 *
 *      // you can do other resource operations like store/delete to a database
 *
 *      transaction.delistResource(xaResource, XAResource.TMSUCCESS);
 *      tm.commit();
 * } catch (Throwable t) {
 *      t.printStackTrace();
 *      transaction.delistResource(xaResource, XAResource.TMFAIL);
 *      tm.rollback();
 * }
 * </code>
 * </pre>
 *
 * @param <K> key
 * @param <V> value
 * @see BaseMap
 * @see IMap
 */
public interface TransactionalMap<K, V> extends TransactionalObject, BaseMap<K, V> {

    /**
     * Transactional implementation of {@link IMap#containsKey(Object)}.
     *
     * @see IMap#containsKey(Object)
     * @throws NullPointerException if the specified key is null.
     */
    @Override
    boolean containsKey(Object key);

    /**
     * Transactional implementation of {@link IMap#get(Object)}.
     *
     * @see IMap#get(Object)
     * @throws NullPointerException if the specified key is null.
     */
    @Override
    V get(Object key);

    /**
     * Locks the key and then gets and returns the value to which the specified key is mapped.
     * Lock will be released at the end of the transaction (either commit or rollback).
     *
     * @throws NullPointerException         if the specified key is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#get(Object)
     */
    V getForUpdate(Object key);

    /**
     * Transactional implementation of {@link IMap#size()}.
     *
     * @see IMap#size()
     */
    @Override
    int size();

    /**
     * Transactional implementation of {@link IMap#isEmpty()}.
     *
     * @see IMap#isEmpty()
     */
    @Override
    boolean isEmpty();

    /**
     * Transactional implementation of {@link IMap#put(Object, Object)}.
     * <p/>
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     *
     * @throws NullPointerException         if the specified key or value is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#put(Object, Object)
     */
    @Override
    V put(K key, V value);

    /**
     * Transactional implementation of {@link IMap#put(Object, Object, long, java.util.concurrent.TimeUnit)}.
     * <p/>
     * The object to be put will be accessible only in the current transaction context till transaction is committed.
     *
     * @throws NullPointerException         if the specified key, value or timeunit is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#put(Object, Object, long, java.util.concurrent.TimeUnit)
     */
    @Override
    V put(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Transactional implementation of {@link IMap#set(Object, Object)}.
     * <p/>
     * The object to be set will be accessible only in the current transaction context till transaction is committed.
     *
     * @throws NullPointerException         if the specified key or value is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#set(Object, Object)
     */
    @Override
    void set(K key, V value);

    /**
     * Transactional implementation of {@link IMap#putIfAbsent(Object, Object)}.
     * <p/>
     * The object to be put will be accessible only in the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key or value is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#putIfAbsent(Object, Object)
     */
    @Override
    V putIfAbsent(K key, V value);

    /**
     * Transactional implementation of {@link IMap#replace(Object, Object)}.
     * <p/>
     * The object to be replaced will be accessible only in the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key or null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#replace(Object, Object)
     */
    @Override
    V replace(K key, V value);

    /**
     * Transactional implementation of {@link IMap#replace(Object, Object, Object)}.
     * <p/>
     * The object to be replaced will be accessible only in the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key, oldValue or newValue is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#replace(Object, Object, Object)
     */
    @Override
    boolean replace(K key, V oldValue, V newValue);

    /**
     * Transactional implementation of {@link IMap#remove(Object)}.
     * <p/>
     * The object to be removed will be removed from only the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#remove(Object)
     */
    @Override
    V remove(Object key);

    /**
     * Transactional implementation of {@link IMap#delete(Object)}.
     * <p/>
     * The object to be deleted will be removed from only the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key is null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#delete(Object)
     */
    @Override
    void delete(Object key);

    /**
     * Transactional implementation of {@link IMap#remove(Object, Object)}.
     * <p/>
     * The object to be removed will be removed from only the current transaction context until the transaction is committed.
     *
     * @throws NullPointerException         if the specified key or value null.
     * @throws TransactionTimedOutException if the key could not be locked for update in the transaction timeout
     * @see IMap#remove(Object, Object)
     */
    @Override
    boolean remove(Object key, Object value);

    /**
     * Transactional implementation of {@link IMap#keySet()}.
     *
     * @see IMap#keySet()
     */
    @Override
    Set<K> keySet();

    /**
     * Transactional implementation of {@link IMap#keySet(com.hazelcast.query.Predicate)}.
     *
     * @see IMap#keySet(com.hazelcast.query.Predicate)
     * @throws NullPointerException if the specified predicate is null.
     */
    @Override
    Set<K> keySet(Predicate predicate);

    /**
     * Transactional implementation of {@link IMap#values()}.
     *
     * @see IMap#values()
     */
    @Override
    Collection<V> values();

    /**
     * Transactional implementation of {@link IMap#values(com.hazelcast.query.Predicate)}.
     *
     * @see IMap#values(com.hazelcast.query.Predicate)
     * @throws NullPointerException if the specified predicate is null.
     */
    @Override
    Collection<V> values(Predicate predicate);
}
