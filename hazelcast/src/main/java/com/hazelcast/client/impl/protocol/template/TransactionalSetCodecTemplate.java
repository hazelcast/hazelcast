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

@GenerateCodec(id = TemplateConstants.TX_SET_TEMPLATE_ID,
        name = "TransactionalSet", ns = "Hazelcast.Client.Protocol.Codec")
public interface TransactionalSetCodecTemplate {
    /**
     * Add new item to transactional set.
     *
     * @param name Name of the Transactional Set
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param item Item added to transactional set
     * @return True if item is added successfully
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object add(String name, String txnId, long threadId, Data item);
    /**
     * Remove item from transactional set.
     *
     * @param name Name of the Transactional Set
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param item Item removed from Transactional Set
     * @return True if item is remove succesfully
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object remove(String name, String txnId, long threadId, Data item);
    /**
     * Returns the size of the set.
     *
     * @param name Name of the Transactional Set
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The size of the set
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.INTEGER)
    Object size(String name, String txnId, long threadId);

}
