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

package com.hazelcast.transaction;

import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;

/**
 * Provides a context to access transactional data-structures like the {@link TransactionalMap}.
 */
public interface TransactionalTaskContext {

    /**
     * Returns the transactional distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return transactional distributed map instance with the specified name
     */
    <K, V> TransactionalMap<K, V> getMap(String name);

    /**
     * Returns the transactional queue instance with the specified name.
     *
     * @param name name of the queue
     * @return transactional queue instance with the specified name
     */
    <E> TransactionalQueue<E> getQueue(String name);

    /**
     * Returns the transactional multimap instance with the specified name.
     *
     * @param name name of the multimap
     * @return transactional multimap instance with the specified name
     */
    <K, V> TransactionalMultiMap<K, V> getMultiMap(String name);

    /**
     * Returns the transactional list instance with the specified name.
     *
     * @param name name of the list
     * @return transactional list instance with the specified name
     */
    <E> TransactionalList<E> getList(String name);

    /**
     * Returns the transactional set instance with the specified name.
     *
     * @param name name of the set
     * @return transactional set instance with the specified name
     */
    <E> TransactionalSet<E> getSet(String name);


    <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name);
}
