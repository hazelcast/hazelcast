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

import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.nio.Disposable;

/**
 * Manages the usage of an address space by allocating and freeing blocks of addresses.
 */
public interface MemoryAllocator extends Disposable {

    /**
     * The special value that does not correspond to an actual address and signals the absence
     * of an address.
     */
    long NULL_ADDRESS = 0L;

    /**
     * Allocates a block of addresses with the requested size and returns the base address of the
     * block. The contents of the block will be initialized to the zero value.
     * <p>
     * Guarantees that no subsequent call to {@code allocate()} will allocate a block which
     * overlaps the currently allocated block. The guarantee holds until the block is
     * {@link #free(long, long)}'d.
     *
     * @param size size of the block to allocate
     * @return the base address of the allocated block
     * @throws NativeOutOfMemoryError if there is not enough free memory to satisfy the reallocation request.
     */
    long allocate(long size);

    /**
     * Accepts the base address and size of a previously allocated block and "reallocates" it by either:
     * <ol><li>
     *     Resizing the existing block, if possible. The contents of the block remain unchanged up to the
     *     lesser of the new and old sizes.
     * </li><li>
     *     Allocating a new block of {@code newSize}, copying the contents of the old block
     *     into it (up to the new block's size), then freeing the old block.
     * </li></ol>
     * The part of the new block which extends beyond the size of the current block (if any) will be
     * initialized to the zero value.
     *
     * @param address     base address of the block
     * @param currentSize size of the block
     * @param newSize     requested new block size
     * @return base address of the reallocated block
     * @throws NativeOutOfMemoryError if there is not enough free memory to satisfy the reallocation request.
     */
    long reallocate(long address, long currentSize, long newSize);

    /**
     * Accepts the base address and size of a previously allocated block and sets it free. All the addresses
     * contained in the block are free to be allocated by future allocation/reallocation requests.
     *
     * @param address base address of the block
     * @param size    size of the block
     */
    void free(long address, long size);
}
