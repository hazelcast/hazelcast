/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.AbstractQueue;
import java.util.Iterator;

/**
 * Not Thread-Safe
 *
 * @param <E>
 */
public final class SimpleBoundedQueue<E> extends AbstractQueue<E> {
    final int maxSize;
    final E[] objects;
    int add = 0;
    int remove = 0;
    int size = 0;

    public SimpleBoundedQueue(int maxSize) {
        this.maxSize = maxSize;
        objects = (E[]) new Object[maxSize];
    }

    @Override
    public boolean add(E obj) {
        if (size == maxSize) {
            return false;
        }
        objects[add] = obj;
        add++;
        size++;
        if (add == maxSize) {
            add = 0;
        }
        return true;
    }

    public E pop() {
        if (size == 0)
            return null;
        int last = add - 1;
        E value = objects[last];
        objects[last] = null;
        size--;
        add--;
        return value;
    }

    @Override
    public int size() {
        return size;
    }

    public int remainingCapacity() {
        return maxSize - size;
    }

    public boolean offer(E o) {
        return add(o);
    }

    public E peek() {
        if (size == 0)
            return null;
        return objects[remove];
    }

    public E poll() {
        if (size == 0)
            return null;
        E value = objects[remove];
        objects[remove] = null;
        remove++;
        size--;
        if (remove == maxSize) {
            remove = 0;
        }
        return value;
    }

    @Override
    public void clear() {
        for (int i = 0; i < maxSize; i++) {
            objects[0] = null;
        }
        add = 0;
        remove = 0;
        size = 0;
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
}
