/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory;

/**
 * Access strategy for single memory bytes. Specifies the minimum contract for
 * endianness-aware operations.
 *
 * @param <R> - type of external resource
 */
public interface ByteAccessStrategy<R> {
    /**
     * Return byte from external resource
     *
     * @param resource - resource to access
     * @param offset   - offset of byte
     * @return - byte from resource
     */
    byte getByte(R resource, long offset);

    /**
     * Writes byte to corresponding resource
     *
     * @param resource - resource to access
     * @param offset   - offset of byte
     * @param value    - byte to write
     */
    void putByte(R resource, long offset, byte value);
}
