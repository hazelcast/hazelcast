/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.util.pool;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueBasedObjectPool<E> extends ObjectPool<E> {
    final BlockingQueue<E> queue;
    final Factory<E> factory;

    public QueueBasedObjectPool(int capacity, Factory<E> factory) {
        this.queue = new LinkedBlockingQueue<E>(capacity);
        this.factory = factory;
    }

    public void add(E e) {
        this.queue.add(e);
    }

    public void addAll(Collection<E> c) {
        this.queue.addAll(c);
    }

    @Override
    public E take() {
        E e = queue.poll();
        if (e == null) {
            try {
                e = factory.create();
            } catch (IOException e1) {
                e1.printStackTrace();
                return take();
            }
        }
        return e;
    }

    @Override
    public void release(E e) {
        queue.offer(e);
    }

    @Override
    public int size() {
        return queue.size();
    }
}

