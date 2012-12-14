/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class QueueItemIterator<E> implements Iterator<E> {
    private E[] entries;
    private int current;
    private QueueClientProxy<E> queueProxy;
    private volatile boolean removeCalled = false;

    public QueueItemIterator(E[] entries, QueueClientProxy<E> queueClientProxy) {
        this.entries = entries;
        this.queueProxy = queueClientProxy;
        current = 0;
    }

    public boolean hasNext() {
        return entries.length > current;
    }

    public E next() {
        if (entries.length <= current) {
            throw new NoSuchElementException();
        }
        E entry = entries[current];
        current++;
        removeCalled = false;
        return entry;
    }

    public void remove() {
        if (current <= 0) {
            throw new IllegalStateException("next() method has not yet been called");
        }
        if (removeCalled) {
            throw new IllegalStateException("remove() method has allready bean called");
        }
        removeCalled = true;
        queueProxy.remove(entries[current - 1]);
    }
}
