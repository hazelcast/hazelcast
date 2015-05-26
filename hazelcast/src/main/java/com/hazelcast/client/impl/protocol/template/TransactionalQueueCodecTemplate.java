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

@GenerateCodec(id = TemplateConstants.TX_QUEUE_TEMPLATE_ID,
        name = "TransactionalQueue", ns = "Hazelcast.Client.Protocol.TransactionalQueue")
public interface TransactionalQueueCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void offer(String name, String txnId, long threadId, Data item, long timeout);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.DATA)
    void take(String name, String txnId, long threadId);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    void poll(String name, String txnId, long threadId, long timeout);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    void peek(String name, String txnId, long threadId, long timeout);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.INTEGER)
    void size(String name, String txnId, long threadId);

}
