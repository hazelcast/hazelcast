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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.EXECUTOR_TEMPLATE_ID,
        name = "ExecutorService", ns = "Hazelcast.Client.Protocol.ExecutorService")
public interface ExecutorServiceCodecTemplate {

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     *
     * @param name Name of the executor.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID)
    void shutdown(String name);

    /**
     * Returns <tt>true</tt> if this executor has been shut down.
     *
     * @param name Name of the executor.
     * @return <tt>true</tt> if this executor has been shut down
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object isShutdown(String name);

    /**
     *
     * @param uuid Unique id for the execution.
     * @param partitionId The id of the partition to execute this cancellation request.
     * @param interrupt If true, then the thread interrupt call can be used to cancel the thread, otherwise interrupt can not be used.
     * @return True if cancelled successfully, false otherwise.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object cancelOnPartition(String uuid, int partitionId, boolean interrupt);

    /**
     *
     * @param uuid Unique id for the execution.
     * @param address Address of the host to execute the request on.
     * @param interrupt If true, then the thread interrupt call can be used to cancel the thread, otherwise interrupt can not be used.
     * @return True if cancelled successfully, false otherwise.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object cancelOnAddress(String uuid, Address address, boolean interrupt);

    /**
     *
     * @param name Name of the executor.
     * @param uuid Unique id for the execution.
     * @param callable The callable object to be executed.
     * @param partitionId The id of the partition to execute this cancellation request.
     * @return The result of the callable execution.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.DATA)
    Object submitToPartition(String name, String uuid, Data callable, int partitionId);

    /**
     *
     * @param name Name of the executor.
     * @param uuid Unique id for the execution.
     * @param callable The callable object to be executed.
     * @param address The member host on which the callable shall be executed on.
     * @return The result of the callable execution.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    Object submitToAddress(String name, String uuid, Data callable, Address address);
}
