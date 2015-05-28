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

@GenerateCodec(id = TemplateConstants.ATOMIC_LONG_TEMPLATE_ID,
        name = "AtomicLong", ns = "Hazelcast.Client.Protocol.AtomicLong")
public interface
        AtomicLongCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    void apply(String name, Data function);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.LONG)
    void alter(String name, Data function);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.LONG)
    void alterAndGet(String name, Data function);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.LONG)
    void getAndAlter(String name, Data function);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.LONG)
    void addAndGet(String name, long delta);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void compareAndSet(String name, long expected, long updated);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.LONG)
    void decrementAndGet(String name);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.LONG)
    void get(String name);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.LONG)
    void getAndAdd(String name, long delta);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.LONG)
    void getAndSet(String name, long newValue);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.LONG)
    void incrementAndGet(String name);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.LONG)
    void getAndIncrement(String name);

    @Request(id = 13, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, long newValue);
}
