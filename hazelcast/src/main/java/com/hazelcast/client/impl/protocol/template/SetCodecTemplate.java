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

@GenerateCodec(id = TemplateConstants.SET_TEMPLATE_ID, name = "Set", ns = "Hazelcast.Client.Protocol.Set")
public interface SetCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.INTEGER)
    void size(String name);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void contains(String name, Data value);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void containsAll(String name, Set<Data> valueSet);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void add(String name, Data value);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void remove(String name, Data value);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void addAll(String name, List<Data> valueList);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndRemoveAll(String name, Set<Data> valueSet);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndRetainAll(String name, Set<Data> valueSet);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.LIST_DATA)
    void getAll(String name);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.STRING,
    event = {EventMessageConst.EVENT_ITEM})
    void addListener(String name, boolean includeValue);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeListener(String name, String registrationId);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void isEmpty(String name);

}
