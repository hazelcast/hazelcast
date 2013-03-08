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

import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;

public interface TransactionalMap<K, V> extends TransactionalObject {

    boolean containsKey(Object key) throws TransactionException;

    V get(Object key) throws TransactionException;

    V put(K key, V value) throws TransactionException;

    void set(K key, V value) throws TransactionException;

    V putIfAbsent(K key, V value) throws TransactionException;

    V replace(K key, V value) throws TransactionException;

    boolean replace(K key, V oldValue, V newValue) throws TransactionException;

    V remove(Object key) throws TransactionException;

    void delete(Object key) throws TransactionException;

    boolean remove(Object key, Object value) throws TransactionException;
}
