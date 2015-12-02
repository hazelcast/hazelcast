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

@GenerateCodec(id = TemplateConstants.COUNTDOWN_LATCH_TEMPLATE_ID,
        name = "CountDownLatch", ns = "Hazelcast.Client.Protocol.Codec")
public interface CountdownLatchCodecTemplate {
    /**
     * Causes the current thread to wait until the latch has counted down to zero, or an exception is thrown, or the
     * specified waiting time elapses. If the current count is zero then this method returns immediately with the value
     * true. If the current count is greater than zero, then the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of five things happen: the count reaches zero due to invocations of the
     * #countDown method, this ICountDownLatch instance is destroyed, the countdown owner becomes disconnected, some
     * other thread Thread#interrupt interrupts the current thread, or the specified waiting time elapses. If the count
     * reaches zero, then the method returns with the value true. If the current thread: has its interrupted status set
     * on entry to this method, or is Thread#interrupt interrupted while waiting, then INTERRUPTED is thrown
     * and the current thread's interrupted status is cleared. If the specified waiting time elapses then the value false
     * is returned.  If the time is less than or equal to zero, the method will not wait at all.
     *
     * @param name    Name of the CountDownLatch
     * @param timeout The maximum time in milliseconds to wait
     * @return True if the count reached zero, false if the waiting time elapsed before the count reached zero
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object await(String name, long timeout);

    /**
     * Decrements the count of the latch, releasing all waiting threads if the count reaches zero. If the current count
     * is greater than zero, then it is decremented. If the new count is zero: All waiting threads are re-enabled for
     * thread scheduling purposes, and Countdown owner is set to null. If the current count equals zero, then nothing happens.
     *
     * @param name Name of the CountDownLatch
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void countDown(String name);

    /**
     * Returns the current count.
     *
     * @param name Name of the CountDownLatch
     * @return The current count for the latch.
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object getCount(String name);

    /**
     * Sets the count to the given value if the current count is zero. If the count is not zero, then this method does
     * nothing and returns false
     *
     * @param name  Name of the CountDownLatch
     * @param count The number of times countDown must be invoked before threads can pass through await
     * @return True if the new count was set, false if the current count is not zero.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object trySetCount(String name, int count);

}
