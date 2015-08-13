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
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.ATOMIC_REFERENCE_TEMPLATE_ID,
        name = "AtomicReference", ns = "Hazelcast.Client.Protocol.AtomicReference")
public interface AtomicReferenceCodecTemplate {

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param function the function applied on the value, the stored value does not change
     * @return  the result of the function application
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    Object apply(String name, Data function);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void alter(String name, Data function);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     * @return the new value, the result of the applied function.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    Object alterAndGet(String name, Data function);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     * @return  the old value, the value before the function is applied
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    Object getAndAlter(String name, Data function);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param expected the value to check (is allowed to be null).
     * @return true if the value is found, false otherwise.
     */
    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object contains(String name, @Nullable Data expected);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param expected the expected value
     * @param updated the new value
     * @return true if successful; or false if the actual value
     *         was not equal to the expected value.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndSet(String name, @Nullable Data expected, @Nullable Data updated);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @return the current value
     */
    @Request(id = 8, retryable = true, response = ResponseMessageConst.DATA)
    Object get(String name);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param newValue the new value
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, @Nullable Data newValue);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param newValue the new value.
     * @return the old value.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA)
    Object getAndSet(String name, @Nullable Data newValue);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @param newValue the new value
     * @return  the new value
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.DATA)
    Object setAndGet(String name, @Nullable Data newValue);

    /**
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @return true if null, false otherwise.
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isNull(String name);
}
