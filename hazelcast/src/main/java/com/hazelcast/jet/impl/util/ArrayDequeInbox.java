/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.Inbox;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * An {@link Inbox} implementation backed by an {@link ArrayDeque}.
 */
public final class ArrayDequeInbox implements Inbox {

    private final ProgressTracker progTracker;
    private final ArrayDeque<Object> queue = new ArrayDeque<>();

    /**
     * Constructs the inbox with the provided progress tracker.
     */
    public ArrayDequeInbox(ProgressTracker progTracker) {
        this.progTracker = progTracker;
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Nonnull @Override
    public Iterator<Object> iterator() {
        return queue.iterator();
    }

    @Override
    public Object peek() {
        return queue.peek();
    }

    @Override
    public Object poll() {
        Object result = queue.poll();
        progTracker.madeProgress(result != null);
        return result;
    }

    @Override
    public void remove() {
        queue.remove();
        progTracker.madeProgress();
    }

    @Override
    public void clear() {
        queue.clear();
        progTracker.madeProgress();
    }

    /**
     * Retrieves the queue backing this inbox.
     */
    public Deque<Object> queue() {
        return queue;
    }

    @Override
    public int size() {
        return queue.size();
    }
}
