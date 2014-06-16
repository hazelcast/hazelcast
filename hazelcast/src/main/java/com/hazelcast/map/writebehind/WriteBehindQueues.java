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

package com.hazelcast.map.writebehind;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class providing static factory methods that create write behind queues.
 */
public final class WriteBehindQueues {

    private WriteBehindQueues() {
    }

    public static <T> WriteBehindQueue<T> createBoundedArrayWriteBehindQueue(int maxSizePerNode, AtomicInteger counter) {
        return new BoundedArrayWriteBehindQueue<T>(maxSizePerNode, counter);
    }

    public static <T> WriteBehindQueue<T> createDefaultWriteBehindQueue(int maxSizePerNode, AtomicInteger counter) {
        return (WriteBehindQueue<T>) createSafeWriteBehindQueue(createBoundedArrayWriteBehindQueue(maxSizePerNode, counter));
    }

    public static <T> WriteBehindQueue<T> emptyWriteBehindQueue() {
        return (WriteBehindQueue<T>) EmptyWriteBehindQueueHolder.EMPTY_WRITE_BEHIND_QUEUE;
    }

    public static <T> WriteBehindQueue<T> createSafeWriteBehindQueue(WriteBehindQueue<T> queue) {
        return new SynchronizedWriteBehindQueue<T>(queue);
    }

    /**
     * Holder provides lazy initialization for singleton instance.
     */
    private static final class EmptyWriteBehindQueueHolder {
        /**
         * Neutral null empty queue.
         */
        private static final WriteBehindQueue EMPTY_WRITE_BEHIND_QUEUE = new EmptyWriteBehindQueue();
    }

    /**
     * Empty write behind queue provides neutral null behaviour.
     */
    private static final class EmptyWriteBehindQueue<T> implements WriteBehindQueue<T> {

        @Override
        public boolean offer(T t) {
            return false;
        }

        @Override
        public void removeFirst() {
        }

        @Override
        public T get(int index) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        @Override
        public T remove(int index) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void clear() {

        }

        @Override
        public WriteBehindQueue<T> getSnapShot() {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }

        @Override
        public void addFront(Collection collection) {

        }

        @Override
        public void addEnd(Collection collection) {

        }

        @Override
        public List removeAll() {
            return Collections.emptyList();
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public List asList() {
            return Collections.emptyList();
        }

        @Override
        public void shrink() {

        }
    }

}
