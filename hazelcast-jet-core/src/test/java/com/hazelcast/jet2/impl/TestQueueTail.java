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

package com.hazelcast.jet2.impl;

import java.util.ArrayList;
import java.util.List;

public class TestQueueTail<T> implements QueueTail<T> {

    private final ArrayList<Object> buffer;
    private final int capacity;

    public TestQueueTail(int capacity) {
        this.capacity = capacity;
        this.buffer = new ArrayList<>(capacity);
    }

    @Override
    public boolean offer(Object item) {
        if (buffer.size() == capacity) {
            return false;
        }
        buffer.add(item);
        return true;
    }

    public List<Object> drain() {
        List<Object> copy = new ArrayList<>(this.buffer);
        this.buffer.clear();
        return copy;
    }

    public List<Object> getBuffer() {
        return buffer;
    }

}
