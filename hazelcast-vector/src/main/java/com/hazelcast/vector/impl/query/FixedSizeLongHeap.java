/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.query;

import io.github.jbellis.jvector.util.AbstractLongHeap;

/**
 * Heap with predefined maximum size which cannot be exceeded.
 * Variant of {@link io.github.jbellis.jvector.util.BoundedLongHeap} that does not allow overflow
 * and does not discard nor replace entries in such situations.
 */
class FixedSizeLongHeap extends AbstractLongHeap {
    private final int maxSize;
    /**
     * Create an empty heap with the configured fixed size.
     *
     * @param maxSize the initial size of the heap
     */
    FixedSizeLongHeap(int maxSize) {
        super(maxSize);
        this.maxSize = maxSize;
    }

    @Override
    public boolean push(long value) {
        if (size >= maxSize) {
            throw new IllegalStateException("Heap is full");
        }
        add(value);
        return true;
    }
}
