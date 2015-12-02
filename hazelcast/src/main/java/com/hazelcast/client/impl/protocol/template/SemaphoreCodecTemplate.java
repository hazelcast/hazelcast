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
     * Try to initialize this ISemaphore instance with the given permit count
     *
     * @param name    Name of the Semaphore
     * @param permits The given permit count
     * @return True if initialization succeeds, false otherwise.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object init(String name, int permits);

    /**
     * Acquires the given number of permits if they are available, and returns immediately, reducing the number of
     * available permits by the given amount. If insufficient permits are available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until one of three things happens: some other thread
     * invokes one of the methods for this semaphore, the current thread is next to be assigned permits and the number
     * of available permits satisfies this request, this ISemaphore instance is destroyed, or some other thread
     * the current thread. If the current thread has its interrupted status set on entry to this method, or is  while
     * waiting for a permit, then  is thrown and the current thread's interrupted status is cleared.
     *
     * @param name    Name of the Semaphore
     * @param permits The given permit count
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void acquire(String name, int permits);

    /**
     * Returns the current number of permits currently available in this semaphore. This method is typically used for
     * debugging and testing purposes.
     *
     * @param name Name of the Semaphore
     * @return The number of permits available in this semaphore.
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object availablePermits(String name);

    /**
     * Acquires and returns all permits that are immediately available.
     *
     * @param name Name of the Semaphore
     * @return The number of permits drained
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object drainPermits(String name);

    /**
     * Shrinks the number of available permits by the indicated reduction. This method differs from  acquire in that it
     * does not block waiting for permits to become available.
     *
     * @param name      Name of the Semaphore
     * @param reduction The number of permits to remove
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void reducePermits(String name, int reduction);

    /**
     * Releases the given number of permits, increasing the number of available permits by that amount. There is no
     * requirement that a thread that releases a permit must have acquired that permit by calling one of the
     * acquire()acquire methods. Correct usage of a semaphore is established by programming convention in the application.
     *
     * @param name    Name of the Semaphore
     * @param permits The number of permits to remove
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void release(String name, int permits);

    /**
     * Acquires the given number of permits, if they are available, and returns immediately, with the value true,
     * reducing the number of available permits by the given amount. If insufficient permits are available then this
     * method will return immediately with the value false and the number of available permits is unchanged.
     *
     * @param name    Name of the Semaphore
     * @param permits The number of permits to remove
     * @param timeout The maximum time to wait for a permit
     * @return true if all permits were acquired,  false if the waiting time elapsed before all permits could be acquired
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object tryAcquire(String name, int permits, long timeout);

}

