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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.IOUtil;

import java.io.Closeable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueBasedObjectPool<E extends Closeable> implements ObjectPool<E> {

    private final BlockingQueue<E> queue;
    private final Factory<E> factory;

    public QueueBasedObjectPool(int capacity, Factory<E> factory) {
        this.queue = new LinkedBlockingQueue<E>(capacity);
        this.factory = factory;
    }

    public E take() {
        // TODO: sync with destroy
        E e = queue.poll();
        if (e == null) {
            try {
                e = factory.create();
            } catch (Exception ex) {
                throw new HazelcastException(ex);
            }
        }
        return e;
    }

    public void release(E e) {
        // TODO: sync with destroy
        queue.offer(e);
    }

    public int size() {
        return queue.size();
    }

    public void destroy() {
        final Collection<E> c = new LinkedList<E>();
        queue.drainTo(c);
        for (E e : c) {
            IOUtil.closeResource(e);
        }
    }
}

