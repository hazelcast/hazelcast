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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NoneStrictObjectPool<T> {
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();
    private final AtomicInteger size = new AtomicInteger();
    private final int maxSize;

    public NoneStrictObjectPool(int maxSize) {
        this.maxSize = maxSize;
    }

    public boolean release(T t) {
        onRelease(t);
        if (size.get() < maxSize) {
            queue.offer(t);
            size.incrementAndGet();
            return true;
        }
        return false;
    }

    public T obtain() {
        T t = queue.poll();
        if (t == null) {
            t = createNew();
        } else {
            size.decrementAndGet();
            onObtain(t);
        }
        return t;
    }

    public abstract T createNew();

    public abstract void onRelease(T t);

    public abstract void onObtain(T t);

    public void clear() {
        queue.clear();
    }
}
