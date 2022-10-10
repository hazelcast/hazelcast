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

package com.hazelcast.internal.alto;

import com.hazelcast.internal.alto.util.Allocator;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public final class OffheapAllocator implements Allocator {

    private final Unsafe unsafe = UnsafeUtil.UNSAFE;

    @Override
    public long allocate(long size) {
        return unsafe.allocateMemory(size);
    }

    @Override
    public long callocate(long size) {
        long address = allocate(size);
        unsafe.setMemory(address, size, (byte) 0);
        return address;
    }

    @Override
    public long reallocate(long address, long bytes) {
        return unsafe.reallocateMemory(address, bytes);
    }

    @Override
    public void free(long address) {
        unsafe.freeMemory(address);
    }
}
