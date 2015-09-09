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

@GenerateCodec(id = TemplateConstants.SEMAPHORE_TEMPLATE_ID,
        name = "Semaphore", ns = "Hazelcast.Client.Protocol.Codec")
public interface SemaphoreCodecTemplate {
    /**
     *
     * @param name Name of the Semaphore
     * @param permits The given permit count
     * @return True if initialization succeeds, false otherwise.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object init(String name, int permits);

    /**
     *
     * @param name Name of the Semaphore
     * @param permits The given permit count
     *
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void acquire(String name, int permits);

    /**
     *
     * @param name Name of the Semaphore
     * @return The number of permits available in this semaphore.
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    Object availablePermits(String name);

    /**
     *
     * @param name Name of the Semaphore
     * @return The number of permits drained
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.INTEGER)
    Object drainPermits(String name);

    /**
     *
     * @param name Name of the Semaphore
     * @param reduction The number of permits to remove
     *
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void reducePermits(String name, int reduction);

    /**
     *
     * @param name Name of the Semaphore
     * @param permits The number of permits to remove
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID)
    void release(String name, int permits);

    /**
     *
     * @param name Name of the Semaphore
     * @param permits The number of permits to remove
     * @param timeout The maximum time to wait for a permit
     * @return true if all permits were acquired,  false if the waiting time elapsed before all permits could be acquired
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object tryAcquire(String name, int permits, long timeout);

}

