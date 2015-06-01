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

@GenerateCodec(id = TemplateConstants.MULTIMAP_TEMPLATE_ID, name = "MultiMap", ns = "Hazelcast.Client.Protocol.MultiMap")
public interface MultiMapCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void put(String name, Data key, Data value, long threadId);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void get(String name, Data key, long threadId);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void remove(String name, Data key, long threadId);

    @Request(id = 4, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void keySet(String name);

    @Request(id = 5, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void values(String name);

    @Request(id = 6, retryable = true, response = ResponseMessageConst.SET_ENTRY)
    void entrySet(String name);

    @Request(id = 7, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsKey(String name, Data key, long threadId);

    @Request(id = 8, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsValue(String name, Data value);

    @Request(id = 9, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsEntry(String name, Data key, Data value, long threadId);

    @Request(id = 10, retryable = true, response = ResponseMessageConst.INTEGER)
    void size(String name);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 12, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void count(String name, Data key, long threadId);

    @Request(id = 13, retryable = true, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListenerToKey(String name, Data key, boolean includeValue);

    @Request(id = 14, retryable = true, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListener(String name, boolean includeValue);

    @Request(id = 15, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeEntryListener(String name, String registrationId);

    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID)
    void lock(String name, Data key, long threadId, long ttl);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void tryLock(String name, Data key, long threadId, long timeout);

    @Request(id = 18, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isLocked(String name, Data key);

    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID)
    void unlock(String name, Data key, long threadId);

    @Request(id = 20, retryable = false, response = ResponseMessageConst.VOID)
    void forceUnlock(String name, Data key);

    @Request(id = 21, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeEntry(String name, Data key, Data value, long threadId);

    @Request(id = 22, retryable = true, response = ResponseMessageConst.INTEGER)
    void valueCount(String name, Data key, long threadId);
}

