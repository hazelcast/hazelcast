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

@GenerateCodec(id = TemplateConstants.EXECUTOR_TEMPLATE_ID,
        name = "ExecutorService", ns = "Hazelcast.Client.Protocol.ExecutorService")
public interface ExecutorServiceCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID)
    void shutdown(String name);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void isShutdown(String name);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void cancelOnPartition(String uuid, int partitionId, boolean interrupt);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void cancelOnAddress(String uuid, String hostname, int port, boolean interrupt);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.DATA)
    void submitToPartition(String name, String uuid, Data callable, int partitionId);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    void submitToAddress(String name, String uuid, Data callable, String hostname, int port);
}
