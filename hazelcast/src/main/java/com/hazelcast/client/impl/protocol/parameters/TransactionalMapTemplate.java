/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

@GenerateParameters(id = 16, name = "TransactionalMap", ns = "Hazelcast.Client.Protocol.TransactionalMap")
public interface TransactionalMapTemplate {

    @EncodeMethod(id = 1)
    void containsKey(String name, String txnId, long threadId, Data key);

    @EncodeMethod(id = 2)
    void get(String name, String txnId, long threadId, Data key);

    @EncodeMethod(id = 3)
    void getForUpdate(String name, String txnId, long threadId, Data key);

    @EncodeMethod(id = 4)
    void size(String name, String txnId, long threadId);

    @EncodeMethod(id = 5)
    void isEmpty(String name, String txnId, long threadId);

    @EncodeMethod(id = 6)
    void put(String name, String txnId, long threadId, Data key, Data value, long ttl);

    @EncodeMethod(id = 7)
    void set(String name, String txnId, long threadId, Data key, Data value);

    @EncodeMethod(id = 8)
    void putIfAbsent(String name, String txnId, long threadId, Data key, Data value);

    @EncodeMethod(id = 9)
    void replace(String name, String txnId, long threadId, Data key, Data value);

    @EncodeMethod(id = 10)
    void replaceIfSame(String name, String txnId, long threadId, Data key, Data oldValue, Data newValue);

    @EncodeMethod(id = 11)
    void remove(String name, String txnId, long threadId, Data key);

    @EncodeMethod(id = 12)
    void delete(String name, String txnId, long threadId, Data key);

    @EncodeMethod(id = 13)
    void removeIfSame(String name, String txnId, long threadId, Data key, Data value);

    @EncodeMethod(id = 14)
    void keySet(String name, String txnId, long threadId);

    @EncodeMethod(id = 15)
    void keySetWithPredicate(String name, String txnId, long threadId, Data predicate);

    @EncodeMethod(id = 16)
    void values(String name, String txnId, long threadId);

    @EncodeMethod(id = 17)
    void valuesWithPredicate(String name, String txnId, long threadId, Data predicate);

}
