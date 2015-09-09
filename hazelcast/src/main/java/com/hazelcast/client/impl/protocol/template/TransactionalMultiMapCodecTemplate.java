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

@GenerateCodec(id = TemplateConstants.TX_MULTIMAP_TEMPLATE_ID,
        name = "TransactionalMultiMap", ns = "Hazelcast.Client.Protocol.Codec")
public interface TransactionalMultiMapCodecTemplate {
    /**
     *
     * @param name Name of the Transactional Multi Map
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param key The key to be stored
     * @param value The value to be stored
     * @return True if the size of the multimap is increased, false if the multimap already contains the key-value pair.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object put(String name, String txnId, long threadId, Data key, Data value);

    /**
     *
     * @param name Name of the Transactional Multi Map
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param key The key whose associated values are returned
     * @return The collection of the values associated with the key
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object get(String name, String txnId, long threadId, Data key);

    /**
     *
     * @param name Name of the Transactional Multi Map
     * @param txnId ID of the transaction
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param key The key whose associated values are returned
     * @return True if the size of the multimap changed after the remove operation, false otherwise.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object remove(String name, String txnId, long threadId, Data key);

    /**
     *
     * @param name Name of the Transactioanal Multi Map
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param key The key whose associated values are returned
     * @param value The value to be stored
     * @return True if the size of the multimap changed after the remove operation, false otherwise.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeEntry(String name, String txnId, long threadId, Data key, Data value);

    /**
     *
     * @param name Name of the Transactioanal Multi Map
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param  key The key whose number of values are returned
     * @return The number of values matching the given key in the multimap
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.INTEGER)
    Object valueCount(String name, String txnId, long threadId, Data key);

    /**
     *
     * @param name Name of the Transactional Multi Map
     * @param txnId ID of the this transaction operation
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The number of key-value pairs in the multimap
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.INTEGER)
    Object size(String name, String txnId, long threadId);

}
