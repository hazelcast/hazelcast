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
        name = "TransactionalQueue", ns = "Hazelcast.Client.Protocol.Codec")
public interface TransactionalQueueCodecTemplate {
    /**
     *
     * @param name Name of the Transcational Queue
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param item The elemet to add
     * @param timeout How long to wait before giving up, in milliseconds
     * @return <tt>true</tt> if successful, or <tt>false</tt> if the specified waiting time elapses before space is available
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object offer(String name, String txnId, long threadId, Data item, long timeout);

    /**
     *
     * @param name Name of the Transactional Queue
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The head of this queue
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.DATA)
    Object take(String name, String txnId, long threadId);

    /**
     *
     * @param name Name of the Transactional Queue
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout How long to wait before giving up, in milliseconds
     * @return The head of this queue, or <tt>null</tt> if the pecified waiting time elapses before an element is available
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    Object poll(String name, String txnId, long threadId, long timeout);

    /**
     *
     * @param name Name of the Transactional Queue
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout How long to wait before giving up, in milliseconds
     * @return The value at the head of the queue.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    Object peek(String name, String txnId, long threadId, long timeout);

    /**
     *
     * @param name Name of the Transactional Queue
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The number of elements in this collection
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.INTEGER)
    Object size(String name, String txnId, long threadId);

}
