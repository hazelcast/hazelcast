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

import java.util.List;

@GenerateParameters(id = 15, name = "ReplicatedMap", ns = "Hazelcast.Client.Protocol.ReplicatedMap")
public interface ReplicatedMapTemplate {

    @EncodeMethod(id = 1)
    void put(String name, Data key, Data value, long ttl);

    @EncodeMethod(id = 2)
    void size(String name);

    @EncodeMethod(id = 3)
    void isEmpty(String name);

    @EncodeMethod(id = 4)
    void containsKey(String name, Data key);

    @EncodeMethod(id = 5)
    void containsValue(String name, Data value);

    @EncodeMethod(id = 6)
    void get(String name, Data key);

    @EncodeMethod(id = 7)
    void remove(String name, Data key);

    @EncodeMethod(id = 8)
    void putAll(String name, List<Data> key, List<Data> value);

    @EncodeMethod(id = 9)
    void clear(String name);

    @EncodeMethod(id = 10)
    void addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate);

    @EncodeMethod(id = 11)
    void addEntryListenerWithPredicate(String name, Data predicate);

    @EncodeMethod(id = 12)
    void addEntryListenerToKey(String name, Data key);

    @EncodeMethod(id = 13)
    void addEntryListener(String name);

    @EncodeMethod(id = 14)
    void removeEntryListener(String name, String registrationId);

    @EncodeMethod(id = 15)
    void keySet(String name);

    @EncodeMethod(id = 16)
    void values(String name);

    @EncodeMethod(id = 17)
    void entrySet(String name);

}

