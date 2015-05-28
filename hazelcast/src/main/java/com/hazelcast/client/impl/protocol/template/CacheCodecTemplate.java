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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

@GenerateCodec(id = TemplateConstants.JCACHE_TEMPLATE_ID, name = "Cache", ns = "Hazelcast.Client.Protocol.Cache")
public interface CacheCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.STRING)
    void addEntryListener(String name);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_CACHEINVALIDATION})
    void addInvalidationListener(String name);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.VOID)
    void removeAll(String name, Set<Data> keys, int completionId);

    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsKey(String name, Data key);

    @Request(id = 6, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void createConfig(Data cacheConfig, boolean createAlsoOnOthers);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA)
    void destroy(String name);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    void entryProcessor(String name, Data key, Data entryProcessor, List<Data> arguments);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void getAll(String name, Set<Data> keys, Data expiryPolicy);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.DATA)
    void getAndRemove(String name, Data key);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA)
    void getAndReplace(String name, Data key, Data value, Data expiryPolicy);

    @Request(id = 12, retryable = true, response = ResponseMessageConst.DATA)
    void getConfig(String name, String simpleName);

    @Request(id = 13, retryable = true, response = ResponseMessageConst.DATA)
    void get(String name, Data key, Data expiryPolicy);

    @Request(id = 14, retryable = false, response = ResponseMessageConst.DATA)
    void iterate(String name, int partitionId, int tableIndex, int batch);

    @Request(id = 15, retryable = false, response = ResponseMessageConst.STRING)
    void listenerRegistration(String name, Data listenerConfig, boolean register, String hostname, int port);

    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID)
    void loadAll(String name, Set<Data> keys, boolean replaceExistingValues);

    @Request(id = 17, retryable = true, response = ResponseMessageConst.DATA)
    void managementConfig(String name, boolean isStat, boolean enabled, String hostname, int port);

    @Request(id = 18, retryable = false, response = ResponseMessageConst.DATA)
    void putIfAbsent(String name, Data key, Data value, Data expiryPolicy);

    @Request(id = 19, retryable = false, response = ResponseMessageConst.DATA)
    void put(String name, Data key, Data value, Data expiryPolicy, boolean get);

    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeEntryListener(String name, String registrationId);

    @Request(id = 21, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeInvalidationListener(String name, String registrationId);

    @Request(id = 22, retryable = false, response = ResponseMessageConst.DATA)
    void remove(String name, Data key, Data currentValue);

    @Request(id = 23, retryable = false, response = ResponseMessageConst.DATA)
    void replace(String name, Data key, Data oldValue, Data newValue, Data expiryPolicy);

    @Request(id = 24, retryable = true, response = ResponseMessageConst.INTEGER)
    void size(String name);

}
