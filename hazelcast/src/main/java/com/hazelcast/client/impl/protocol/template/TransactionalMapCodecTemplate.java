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
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.TX_MAP_TEMPLATE_ID,
        name = "TransactionalMap", ns = "Hazelcast.Client.Protocol.TransactionalMap")
public interface TransactionalMapCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void containsKey(String name, String txnId, long threadId, Data key);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.DATA)
    void get(String name, String txnId, long threadId, Data key);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    void getForUpdate(String name, String txnId, long threadId, Data key);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.INTEGER)
    void size(String name, String txnId, long threadId);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void isEmpty(String name, String txnId, long threadId);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    void put(String name, String txnId, long threadId, Data key, Data value, long ttl);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, String txnId, long threadId, Data key, Data value);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    void putIfAbsent(String name, String txnId, long threadId, Data key, Data value);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.DATA)
    void replace(String name, String txnId, long threadId, Data key, Data value);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void replaceIfSame(String name, String txnId, long threadId, Data key, Data oldValue, Data newValue);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA)
    void remove(String name, String txnId, long threadId, Data key);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID)
    void delete(String name, String txnId, long threadId, Data key);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeIfSame(String name, String txnId, long threadId, Data key, Data value);

    @Request(id = 14, retryable = false, response = ResponseMessageConst.SET_DATA)
    void keySet(String name, String txnId, long threadId);

    @Request(id = 15, retryable = false, response = ResponseMessageConst.SET_DATA)
    void keySetWithPredicate(String name, String txnId, long threadId, Data predicate);

    @Request(id = 16, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void values(String name, String txnId, long threadId);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void valuesWithPredicate(String name, String txnId, long threadId, Data predicate);

}
