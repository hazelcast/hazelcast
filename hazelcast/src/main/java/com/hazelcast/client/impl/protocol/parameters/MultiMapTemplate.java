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

@GenerateParameters(id = 2, name = "MultiMap", ns = "Hazelcast.Client.Protocol.MultiMap")
public interface MultiMapTemplate {

    @EncodeMethod(id = 1)
    void put(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 2)
    void get(String name, Data key, long threadId);

    @EncodeMethod(id = 3)
    void remove(String name, Data key, long threadId);

    @EncodeMethod(id = 4)
    void keySet(String name);

    @EncodeMethod(id = 5)
    void values(String name);

    @EncodeMethod(id = 6)
    void entrySet(String name);

    @EncodeMethod(id = 7)
    void containsKey(String name, Data key, long threadId);

    @EncodeMethod(id = 8)
    void containsValue(String name, Data value);

    @EncodeMethod(id = 9)
    void containsEntry(String name, Data key, Data value, long threadId);

    @EncodeMethod(id = 10)
    void size(String name);

    @EncodeMethod(id = 11)
    void clear(String name);

    @EncodeMethod(id = 12)
    void count(String name, Data key, long threadId);

    @EncodeMethod(id = 13)
    void addEntryListenerToKey(String name, Data key, boolean includeValue);

    @EncodeMethod(id = 14)
    void addEntryListener(String name, boolean includeValue);

    @EncodeMethod(id = 15)
    void removeEntryListener(String name, String registrationId);

    @EncodeMethod(id = 16)
    void lock(String name, Data key, long threadId, long ttl);

    @EncodeMethod(id = 19)
    void tryLock(String name, Data key, long threadId, long timeout);

    @EncodeMethod(id = 20)
    void isLocked(String name, Data key, long threadId);

    @EncodeMethod(id = 21)
    void unlock(String name, Data key, long threadId);

    @EncodeMethod(id = 22)
    void forceUnlock(String name, Data key, long threadId);

    @EncodeMethod(id = 23)
    void removeEntry(String name, Data key, Data value, long threadId);

    @EncodeMethod(id = 24)
    void valueCount(String name, Data key, long threadId);
}
