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
        name = "AtomicReference", ns = "Hazelcast.Client.Protocol.Codec")
public interface AtomicReferenceCodecTemplate {

    /**
     * Applies a function on the value, the actual stored value will not change.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param function the function applied on the value, the stored value does not change
     * @return the result of the function application
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object apply(String name, Data function);

    /**
     * Alters the currently stored reference by applying a function on it.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void alter(String name, Data function);

    /**
     * Alters the currently stored reference by applying a function on it and gets the result.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     * @return the new value, the result of the applied function.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object alterAndGet(String name, Data function);

    /**
     * Alters the currently stored reference by applying a function on it on and gets the old value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param function the function that alters the currently stored reference
     * @return the old value, the value before the function is applied
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object getAndAlter(String name, Data function);

    /**
     * Checks if the reference contains the value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param expected the value to check (is allowed to be null).
     * @return true if the value is found, false otherwise.
     */
    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object contains(String name, @Nullable Data expected);

    /**
     * Atomically sets the value to the given updated value only if the current value the expected value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param expected the expected value
     * @param updated  the new value
     * @return true if successful; or false if the actual value
     * was not equal to the expected value.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object compareAndSet(String name, @Nullable Data expected, @Nullable Data updated);

    /**
     * Gets the current value.
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @return the current value
     */
    @Request(id = 8, retryable = true, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object get(String name);

    /**
     * Atomically sets the given value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param newValue the new value
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void set(String name, @Nullable Data newValue);

    /**
     * Clears the current stored reference.
     *
     * @param name Name of the AtomicReference distributed object instance.
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void clear(String name);

    /**
     * Gets the old value and sets the new value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param newValue the new value.
     * @return the old value.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object getAndSet(String name, @Nullable Data newValue);

    /**
     * Sets and gets the value.
     *
     * @param name     Name of the AtomicReference distributed object instance.
     * @param newValue the new value
     * @return the new value
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object setAndGet(String name, @Nullable Data newValue);

    /**
     * Checks if the stored reference is null.
     *
     * @param name Name of the AtomicReference distributed object instance.
     * @return true if null, false otherwise.
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object isNull(String name);
}
