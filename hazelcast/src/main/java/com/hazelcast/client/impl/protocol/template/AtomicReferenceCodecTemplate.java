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
import com.hazelcast.client.impl.protocol.parameters.TemplateConstants;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.ATOMIC_REFERENCE_TEMPLATE_ID,
        name = "AtomicReference", ns = "Hazelcast.Client.Protocol.AtomicReference")
public interface AtomicReferenceCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    void apply(String name, Data function);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void alter(String name, Data function);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    void alterAndGet(String name, Data function);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    void getAndAlter(String name, Data function);

    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void contains(String name, Data expected);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndSet(String name, Data expected, Data updated);

    @Request(id = 8, retryable = true, response = ResponseMessageConst.DATA)
    void get(String name);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, Data newValue);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA)
    void getAndSet(String name, Data newValue);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.DATA)
    void setAndGet(String name, Data newValue);

    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isNull(String name);
}
