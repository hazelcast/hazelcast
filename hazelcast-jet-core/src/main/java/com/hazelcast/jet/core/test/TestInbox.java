/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core.test;

import com.hazelcast.jet.core.Inbox;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

/**
 * {@link Inbox} implementation suitable to be used in tests.
 */
public final class TestInbox implements Inbox {

    private final ArrayDeque<Object> queue = new ArrayDeque<>();

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public Object peek() {
        return queue.peek();
    }

    @Override
    public Object poll() {
        return queue.poll();
    }

    @Override
    public void remove() {
        queue.remove();
    }

    /**
     * Retrieves the queue backing the inbox.
     */
    public Deque<Object> queue() {
        return queue;
    }

    /**
     * Convenience for {@code inbox.queue().add(o)}
     */
    public void add(Object o) {
        queue.add(o);
    }

    /**
     * Convenience for {@code inbox.queue().addAll(collection)}
     */
    public void addAll(Collection<?> collection) {
        queue.addAll(collection);
    }

    /**
     * Convenience for {@code inbox.queue().clear()}
     */
    public void clear() {
        queue.clear();
    }

    /**
     * Convenience for {@code inbox.queue().size()}
     */
    public int size() {
       return queue.size();
    }
}
