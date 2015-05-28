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

import java.util.Map;

@GenerateCodec(id = TemplateConstants.REPLICATED_MAP_TEMPLATE_ID,
        name = "ReplicatedMap", ns = "Hazelcast.Client.Protocol.ReplicatedMap")
public interface ReplicatedMapCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    void put(String name, Data key, Data value, long ttl);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.INTEGER)
    void size(String name);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isEmpty(String name);

    @Request(id = 4, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsKey(String name, Data key);

    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void containsValue(String name, Data value);

    @Request(id = 6, retryable = true, response = ResponseMessageConst.DATA)
    void get(String name, Data key);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA)
    void remove(String name, Data key);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.VOID)
    void putAll(String name, Map<Data, Data> map);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListenerWithPredicate(String name, Data predicate);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListenerToKey(String name, Data key);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    void addEntryListener(String name);

    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeEntryListener(String name, String registrationId);

    @Request(id = 15, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void keySet(String name);

    @Request(id = 16, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void values(String name);

    @Request(id = 17, retryable = true, response = ResponseMessageConst.MAP_DATA_DATA)
    void entrySet(String name);

}

