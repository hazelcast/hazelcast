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
import com.hazelcast.client.impl.protocol.parameters.TemplateConstants;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

@GenerateCodec(id = TemplateConstants.MAP_TEMPLATE_ID, name = "Map", ns = "Hazelcast.Client.Protocol.Map")
public interface MapCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    void put(String name, Data key, Data value, long threadId, long ttl);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.DATA)
    void get(String name, Data key, long threadId);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    void remove(String name, Data key, long threadId);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    void replace(String name, Data key, Data value, long threadId);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void replaceIfSame(String name, Data key, Data testValue, Data value, long threadId);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    void putAsync(String name, Data key, Data value, long threadId, long ttl);

    @Request(id = 7, retryable = true, response = ResponseMessageConst.DATA)
    void getAsync(String name, Data key, long threadId);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    void removeAsync(String name, Data key, long threadId);

    @Request(id = 9, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsKey(String name, Data key, long threadId);

    @Request(id = 10, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsValue(String name, Data value);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeIfSame(String name, Data key, Data value, long threadId);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID)
    void delete(String name, Data key, long threadId);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.VOID)
    void flush(String name);

    @Request(id = 14, retryable = false, response = ResponseMessageConst.DATA)
    void tryRemove(String name, Data key, long threadId, long timeout);

    @Request(id = 15, retryable = false, response = ResponseMessageConst.DATA)
    void tryPut(String name, Data key, Data value, long threadId, long timeout);

    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID)
    void putTransient(String name, Data key, Data value, long threadId, long ttl);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.DATA)
    void putIfAbsent(String name, Data key, Data value, long threadId, long ttl);

    @Request(id = 18, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, Data key, Data value, long threadId, long ttl);

    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID)
    void lock(String name, Data key, long threadId, long ttl);

    @Request(id = 20, retryable = false, response = ResponseMessageConst.DATA)
    void tryLock(String name, Data key, long threadId, long timeout);

    @Request(id = 21, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isLocked(String name, Data key);

    @Request(id = 22, retryable = false, response = ResponseMessageConst.VOID)
    void unlock(String name, Data key, long threadId);

    @Request(id = 23, retryable = false, response = ResponseMessageConst.STRING)
    void addInterceptor(String name, Data interceptor);

    @Request(id = 24, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeInterceptor(String name, String id);

    @Request(id = 25, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRYEVENT)
    void addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate, boolean includeValue);

    @Request(id = 26, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRYEVENT)
    void addEntryListenerWithPredicate(String name, Data predicate, boolean includeValue);

    @Request(id = 27, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRYEVENT)
    void addEntryListenerToKey(String name, Data key, boolean includeValue);

    @Request(id = 28, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRYEVENT)
    void addEntryListener(String name, boolean includeValue);

    @Request(id = 29, retryable = false, response = ResponseMessageConst.DATA)
    void addNearCacheEntryListener(String name, boolean includeValue);

    @Request(id = 30, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeEntryListener(String name, String registrationId);

    @Request(id = 31, retryable = false, response = ResponseMessageConst.STRING,
            event = EventMessageConst.EVENT_PARTITIONLOSTEVENT)
    void addPartitionLostListener(String name);

    @Request(id = 32, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removePartitionLostListener(String name, String registrationId);

    @Request(id = 33, retryable = true, response = ResponseMessageConst.ENTRY_VIEW)
    void getEntryView(String name, Data key, long threadId);

    @Request(id = 34, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void evict(String name, Data key, long threadId);

    @Request(id = 35, retryable = false, response = ResponseMessageConst.VOID)
    void evictAll(String name);

    @Request(id = 36, retryable = false, response = ResponseMessageConst.VOID)
    void loadAll(String name, boolean replaceExistingValues);

    @Request(id = 37, retryable = false, response = ResponseMessageConst.VOID)
    void loadGivenKeys(String name, List<Data> keys, boolean replaceExistingValues);

    @Request(id = 38, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void keySet(String name);

    @Request(id = 39, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void getAll(String name, Set<Data> keys);

    @Request(id = 40, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void values(String name);

    @Request(id = 41, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void entrySet(String name);

    @Request(id = 42, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void keySetWithPredicate(String name, Data predicate);

    @Request(id = 43, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void valuesWithPredicate(String name, Data predicate);

    @Request(id = 44, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void entriesWithPredicate(String name, Data predicate);

    @Request(id = 45, retryable = false, response = ResponseMessageConst.VOID)
    void addIndex(String name, String attribute, boolean ordered);

    @Request(id = 46, retryable = true, response = ResponseMessageConst.INTEGER)
    void size(String name);

    @Request(id = 47, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isEmpty(String name);

    @Request(id = 48, retryable = false, response = ResponseMessageConst.VOID)
    void putAll(String name, List<Data> keys, List<Data> values);

    @Request(id = 49, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 50, retryable = false, response = ResponseMessageConst.DATA)
    void executeOnKey(String name, Data entryProcessor, Data key);

    @Request(id = 51, retryable = false, response = ResponseMessageConst.DATA)
    void submitToKey(String name, Data entryProcessor, Data key);

    @Request(id = 52, retryable = false, response = ResponseMessageConst.DATA)
    void executeOnAllKeys(String name, Data entryProcessor);

    @Request(id = 53, retryable = false, response = ResponseMessageConst.DATA)
    void executeWithPredicate(String name, Data entryProcessor, Data predicate);

    @Request(id = 54, retryable = false, response = ResponseMessageConst.DATA)
    void executeOnKeys(String name, Data entryProcessor, Set<Data> keys);

    @Request(id = 55, retryable = false, response = ResponseMessageConst.VOID)
    void forceUnlock(String name, Data key);

}
