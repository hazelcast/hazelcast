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

package com.hazelcast.jet.memory.spi.memory;

public enum MemoryChainingType {
    HEAP(MemoryType.HEAP, null),
    NATIVE(MemoryType.NATIVE, null),
    HEAP_NATIVE(MemoryType.HEAP, MemoryType.NATIVE),
    NATIVE_HEAP(MemoryType.NATIVE, MemoryType.HEAP);

    private final MemoryType nextMemoryType;
    private final MemoryType currentMemoryType;

    MemoryChainingType(MemoryType currentMemoryType,
                       MemoryType nextMemoryType) {
        this.nextMemoryType = nextMemoryType;
        this.currentMemoryType = currentMemoryType;
    }

    public MemoryType getNextMemoryType() {
        return nextMemoryType;
    }

    public MemoryType getCurrentMemoryType() {
        return currentMemoryType;
    }
}
