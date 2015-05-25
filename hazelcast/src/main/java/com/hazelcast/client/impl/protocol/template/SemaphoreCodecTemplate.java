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

@GenerateCodec(id = TemplateConstants.SEMAPHORE_TEMPLATE_ID,
        name = "Semaphore", ns = "Hazelcast.Client.Protocol.Semaphore")
public interface SemaphoreCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void init(String name, int permits);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void acquire(String name, int permits);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    void availablePermits(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.INTEGER)
    void drainPermits(String name);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void reducePermits(String name, int reduction);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID)
    void release(String name, int permits);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void tryAcquire(String name, int permits, long timeout);

}

