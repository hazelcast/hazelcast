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

@GenerateCodec(id = TemplateConstants.CONDITION_TEMPLATE_ID,
        name = "Condition", ns = "Hazelcast.Client.Protocol.Codec")
public interface ConditionCodecTemplate {
    /**
     *
     * @param name Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout The maximum time to wait
     * @param lockName Name of the lock to wait on.
     * @return False if the waiting time detectably elapsed before return from the method, else true
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object await(String name, long threadId, long timeout, String lockName);

    /**
     *
     * @param name Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void beforeAwait(String name, long threadId, String lockName);

    /**
     *
     * @param name Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID)
    void signal(String name, long threadId, String lockName);

    /**
     *
     * @param name Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.VOID)
    void signalAll(String name, long threadId, String lockName);

}
