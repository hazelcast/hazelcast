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

import java.util.Collection;

@GenerateCodec(id = TemplateConstants.QUEUE_TEMPLATE_ID, name = "Queue", ns = "Hazelcast.Client.Protocol.Queue")
public interface QueueCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void offer(String name, Data value, long timeoutMillis);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void put(String name, Data value);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.INTEGER)
    void size(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void remove(String name, Data value);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.DATA)
    void poll(String name, long timeoutMillis);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    void take(String name);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA)
    void peek(String name);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void iterator(String name);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void drainTo(String name);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void drainToMaxSize(String name, int maxSize);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void contains(String name, Data value);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void containsAll(String name, Collection<Data> dataList);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndRemoveAll(String name, Collection<Data> dataList);

    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndRetainAll(String name, Collection<Data> dataList);

    @Request(id = 15, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 16, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void addAll(String name, Collection<Data> dataList);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ITEMEVENT})
    void addListener(String name, boolean includeValue);

    @Request(id = 18, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeListener(String name, String registrationId);

    @Request(id = 19, retryable = false, response = ResponseMessageConst.INTEGER)
    void remainingCapacity(String name);

    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void isEmpty(String name);

}
