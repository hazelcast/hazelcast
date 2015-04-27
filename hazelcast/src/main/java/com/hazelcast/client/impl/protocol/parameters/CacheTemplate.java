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
import java.util.Set;

@GenerateParameters(id = TemplateConstants.JCACHE_TEMPLATE_ID, name = "Cache", ns = "Hazelcast.Client.Protocol.Cache")
public interface CacheTemplate {

    @EncodeMethod(id = 1)
    void addEntryListener(String name);

    @EncodeMethod(id = 2)
    void addInvalidationListener(String name);

    @EncodeMethod(id = 3)
    void clear(String name);

    @EncodeMethod(id = 4)
    void removeAll(String name, Set<Data> keys, int completionId);

    @EncodeMethod(id = 5)
    void containsKey(String name, Data key);

    @EncodeMethod(id = 6)
    void createConfig(Data cacheConfig, boolean createAlsoOnOthers);

    @EncodeMethod(id = 7)
    void destroy(String name);

    @EncodeMethod(id = 8)
    void entryProcessor(String name, Data key, Data entryProcessor, List<Data> arguments);

    @EncodeMethod(id = 9)
    void getAll(String name, Set<Data> keys, Data expiryPolicy);

    @EncodeMethod(id = 10)
    void getAndRemove(String name, Data key);

    @EncodeMethod(id = 11)
    void getAndReplace(String name, Data key, Data value, Data expiryPolicy);

    @EncodeMethod(id = 12)
    void getConfig(String name, String simpleName);

    @EncodeMethod(id = 13)
    void get(String name, Data key, Data expiryPolicy);

    @EncodeMethod(id = 14)
    void iterate(String name, int partitionId, int tableIndex, int batch);

    @EncodeMethod(id = 15)
    void listenerRegistration(String name, Data listenerConfig, boolean register, String hostname, int port);

    @EncodeMethod(id = 16)
    void loadAll(String name, Set<Data> keys, boolean replaceExistingValues);

    @EncodeMethod(id = 17)
    void managementConfig(String name, boolean isStat, boolean enabled, String hostname, int port);

    @EncodeMethod(id = 18)
    void putIfAbsent(String name, Data key, Data value, Data expiryPolicy);

    @EncodeMethod(id = 19)
    void put(String name, Data key, Data value, Data expiryPolicy, boolean get);

    @EncodeMethod(id = 20)
    void removeEntryListener(String name, String registrationId);

    @EncodeMethod(id = 21)
    void removeInvalidationListener(String name, String registrationId);

    @EncodeMethod(id = 22)
    void remove(String name, Data key, Data currentValue);

    @EncodeMethod(id = 23)
    void replace(String name, Data key, Data oldValue, Data newValue, Data expiryPolicy);

    @EncodeMethod(id = 24)
    void size(String name);

    @EncodeMethod(id = 25)
    void invalidationMessage(String name, Data key, String sourceUuid);

}
