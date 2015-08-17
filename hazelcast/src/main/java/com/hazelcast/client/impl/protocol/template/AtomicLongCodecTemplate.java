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

@GenerateCodec(id = TemplateConstants.ATOMIC_LONG_TEMPLATE_ID,
        name = "AtomicLong", ns = "Hazelcast.Client.Protocol.AtomicLong")
public interface
        AtomicLongCodecTemplate {

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param function The function applied to the value, the value is not changed.
     * @return The result of the function application.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    Object apply(String name, Data function);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param function The function applied to the currently stored value.
     * @return The result of the function application.
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.LONG)
    Object alter(String name, Data function);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param function The function applied to the currently stored value.
     * @return The result of the function application.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.LONG)
    Object alterAndGet(String name, Data function);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param function The function applied to the currently stored value.
     * @return The old value before the function application.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.LONG)
    Object getAndAlter(String name, Data function);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param delta the value to add to the current value
     * @return the updated value, the given value added to the current value
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.LONG)
    Object addAndGet(String name, long delta);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param expected the expected value
     * @param updated the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndSet(String name, long expected, long updated);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @return the updated value, the current value decremented by one
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.LONG)
    Object decrementAndGet(String name);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @return the current value
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.LONG)
    Object get(String name);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param delta the value to add to the current value
     * @return the old value before the add
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.LONG)
    Object getAndAdd(String name, long delta);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param newValue the new value
     * @return the old value
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.LONG)
    Object getAndSet(String name, long newValue);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @return The updated value, the current value incremented by one
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.LONG)
    Object incrementAndGet(String name);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @return the old value
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.LONG)
    Object getAndIncrement(String name);

    /**
     *
     * @param name The name of this IAtomicLong instance.
     * @param newValue The new value
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, long newValue);
}
