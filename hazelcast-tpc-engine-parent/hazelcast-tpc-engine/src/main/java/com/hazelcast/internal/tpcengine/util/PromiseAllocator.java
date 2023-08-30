/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;


import com.hazelcast.internal.tpcengine.Eventloop;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

@SuppressWarnings("rawtypes")
public final class PromiseAllocator {

    private final Eventloop eventloop;
    private final Promise[] array;
    private final int capacity;
    private int index;

    public PromiseAllocator(Eventloop eventloop, int capacity) {
        this.eventloop = checkNotNull(eventloop);
        this.capacity = checkPositive(capacity, "capacity");
        this.array = new Promise[capacity];
        for (int k = 0; k < capacity; k++) {
            Promise promise = new Promise(eventloop);
            promise.allocator = this;
            array[k] = promise;
        }
    }

    public int available() {
        return capacity - index;
    }

    /**
     * Allocates a Promise or null if there is no further space.
     *
     * @return the allocated Promise or null if there is no more space.
     */
    public Promise allocate() {
        if (index == capacity) {
            return null;
        }

        Promise promise = array[index];
        array[index] = null;
        index++;
        promise.refCount = 1;
        return promise;
    }

    void free(Promise e) {
        checkNotNull(e);

        if (index > 0) {
            index--;
            array[index] = e;
        }
    }
}
