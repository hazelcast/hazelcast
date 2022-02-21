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

package com.hazelcast.internal.util.sort;

import com.hazelcast.internal.memory.MemoryAccessor;

/**
 * Base class for {@link QuickSorter} implementations on a memory block accessed by a
 * {@link MemoryAccessor}.
 */
public abstract class MemArrayQuickSorter extends QuickSorter {

    protected final MemoryAccessor mem;
    protected long baseAddress;

    /**
     * @param mem the {@link MemoryAccessor} to use
     * @param baseAddress the initial base address of the block to sort
     */
    protected MemArrayQuickSorter(MemoryAccessor mem, long baseAddress) {
        this.mem = mem;
        this.baseAddress = baseAddress;
    }

    /**
     * Sets the base address to the supplied address.
     * @param baseAddress the supplied address.
     * @return {@code this} the base address.
     */
    public MemArrayQuickSorter gotoAddress(long baseAddress) {
        this.baseAddress = baseAddress;
        return this;
    }
}
