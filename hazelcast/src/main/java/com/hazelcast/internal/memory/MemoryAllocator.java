/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.Disposable;

/**
 * Memory Allocator allocates/free memory blocks from/to OS like C malloc()/free()
 */
public interface MemoryAllocator extends Disposable {

    /**
     * NULL pointer address.
     */
    long NULL_ADDRESS = 0L;

    /**
     * Allocates memory directly from OS, like C malloc().
     * Content of the memory block will be initialized to zero.
     * <p>
     * Complement of {@link #free(long, long)}.
     *
     * @param size requested size of memory block
     * @return address of memory block
     * @throws NativeOutOfMemoryError if not enough memory is available
     */
    long allocate(long size);

    /**
     * Reallocates given memory block. Resizes it to the new size, like C realloc().
     * <p>
     * The reallocation is done by either:
     * <ol>
     * <li>Expanding the existing block, if possible.
     * The contents of the block remain unchanged up to the lesser of the new and old sizes.</li>
     * <li>Allocating a new memory block of newSize,
     * copying memory area with size equal the lesser of the new and the old sizes,
     * and freeing the old block.</li>
     * </ol>
     * <p>
     * If new size is greater than current size then contents
     * of the new part will be initialized to zero.
     *
     * @param address     address of memory block
     * @param currentSize current size of memory block
     * @param newSize     requested size of memory block
     * @return address of memory block
     * @throws NativeOutOfMemoryError if not enough memory is available
     */
    long reallocate(long address, long currentSize, long newSize);

    /**
     * Gives allocated memory block back to OS, like C free().
     * <p>
     * <p>
     * Complement of {@link #allocate(long)}.
     *
     * @param address address of memory block
     * @param size    size of memory block
     */
    void free(long address, long size);
}
