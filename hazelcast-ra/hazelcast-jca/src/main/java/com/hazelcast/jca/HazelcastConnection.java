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

package com.hazelcast.jca;

import java.util.concurrent.ExecutorService;

import javax.resource.cci.Connection;

import com.hazelcast.core.*;
import com.hazelcast.transaction.TransactionalTaskContext;

/** 
 * Hazelcast specific connection which allows
 * access to this hazelcast instance to acquire
 * the maps, lists etc.
 */
public interface HazelcastConnection extends Connection {

    /**
     * @see HazelcastInstance#getQueue(String)
     */
    <E> IQueue<E> getQueue(String name);

    /**
     * @see HazelcastInstance#getTopic(String)
     */
    <E> ITopic<E> getTopic(String name);

    /**
     * @see HazelcastInstance#getSet(String)
     */
    <E> ISet<E> getSet(String name);

    /**
     * @see HazelcastInstance#getList(String)
     */
    <E> IList<E> getList(String name);

    /**
     * @see HazelcastInstance#getMap(String)
     */
    <K, V> IMap<K, V> getMap(String name);

    /**
     * @see HazelcastInstance#getMultiMap(String)
     */
    <K, V> MultiMap<K, V> getMultiMap(String name);
    

    /**
     * @see HazelcastInstance#getExecutorService(String)
     */
    IExecutorService getExecutorService(String name);

    /**
     * @see HazelcastInstance#getAtomicLong(String)
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * @see HazelcastInstance#getCountDownLatch(String)
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * @see HazelcastInstance#getSemaphore(String)
     */
    ISemaphore getSemaphore(String name);


    //Transactionals

    /**
     * @see TransactionalTaskContext#getMap(String)
     */
    <K, V> TransactionalMap<K, V> getTransactionalMap(String name);

    /**
     * @see TransactionalTaskContext#getQueue(String)
     */
    <E> TransactionalQueue<E> getTransactionalQueue(String name);

    /**
     * @see TransactionalTaskContext#getMultiMap(String)
     */
    <K, V> TransactionalMultiMap<K, V> getTransactionalMultiMap(String name);

    /**
     * @see TransactionalTaskContext#getList(String)
     */
    <E> TransactionalList<E> getTransactionalList(String name);

    /**
     * @see TransactionalTaskContext#getSet(String)
     */
    <E> TransactionalSet<E> getTransactionalSet(String name);

}
