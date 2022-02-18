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
 * Accessor of global memory, both in CPU's native address space and on the Java heap.
 * Since this is a global, singleton resource, there should be a global singleton instance
 * of each applicable implementation of this interface.
 * <p>
 * The instance of {@code GlobalMemoryAccessor} appropriate to the platform and intended usage
 * can be retrieved from the {@link GlobalMemoryAccessorRegistry} by supplying the desired
 * {@link GlobalMemoryAccessorType}, or by directly using the constants
 * {@link GlobalMemoryAccessorRegistry#MEM} and {@link GlobalMemoryAccessorRegistry#MEM}.
 * <p>
 * This interface also works as a {@code ByteAccessStrategy<Object>} with the semantics matching the
 * semantics of {@code Unsafe}: when an object is supplied as the "resource", access is relative to
 * that object's base address; when {@code null} is supplied, a base address of zero is implied, i.e.,
 * the supplied "offset" is treated as the absolute address in CPU's native address space.
 *
 * @see ByteAccessStrategy
 * @see GlobalMemoryAccessorType
 * @see GlobalMemoryAccessorRegistry
 */
@SuppressWarnings("checkstyle:interfaceistype")
public interface GlobalMemoryAccessor
        extends ConcurrentMemoryAccessor, ConcurrentHeapMemoryAccessor, ByteAccessStrategy<Object> {

    /**
     * Maximum size of a block of memory to copy in a single low-level memory-copying
     * operation. The goal is to prevent large periods without a GC safepoint.
     */
    int MEM_COPY_THRESHOLD = 1024 * 1024;
}
