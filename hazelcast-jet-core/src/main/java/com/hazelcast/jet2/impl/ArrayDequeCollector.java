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

import com.hazelcast.jet2.OutputCollector;

import java.util.ArrayDeque;

/**
 * Javadoc pending.
 */
public class ArrayDequeCollector<E> implements OutputCollector<E> {
    private final ArrayDeque<E> deque = new ArrayDeque<>();

    @Override
    public void collect(E item) {
        deque.add(item);
    }

    public E peek() {
        return deque.peek();
    }

    public void remove() {
        deque.remove();
    }
}
