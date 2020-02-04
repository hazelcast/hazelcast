/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public class UnsafeAllocator implements MemoryAllocator {
    private final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    public static final UnsafeAllocator INSTANCE = new UnsafeAllocator();

    @Override
    public long allocate(long size) {
        try {
            return UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        try {
            return UNSAFE.reallocateMemory(address, newSize);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address, long size) {
        UNSAFE.freeMemory(address);
    }

    @Override
    public void dispose() {

    }
}
