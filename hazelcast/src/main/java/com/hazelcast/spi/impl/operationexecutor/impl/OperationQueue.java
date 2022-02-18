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

package com.hazelcast.spi.impl.operationexecutor.impl;

/**
 * The OperationQueue is the queue used to schedule operations/tasks on an
 * OperationThread.
 *
 * Implementations must support Multiple-Producer Multiple-Consumers scenario
 * as multiple {@link GenericOperationThread} share a single queue.
 *
 * The ScheduledQueue also support priority tasks; so if a task with a priority
 * comes in, than that one is taken before any other normal operation is taken.
 *
 * The ordering between normal tasks will always be FIFO. And the same goes for
 * the ordering between priority tasks, but there is no ordering guarantee between
 * priority and normal tasks.
 */
public interface OperationQueue {

    /**
     * Adds an task to this queue.
     *
     * This method is thread safe.
     *
     * @param task     the item to add
     * @param priority if the task has priority or not
     * @throws java.lang.NullPointerException if task is null
     */
    void add(Object task, boolean priority);

    /**
     * Takes an item from this queue. If no item is available, the call blocks.
     *
     * This method should always be called by the same thread.
     *
     * @param priorityOnly true if only priority items should be taken. This is
     *                     useful for priority generic threads since they
     *                     should only take priority items.
     * @return the taken item.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    Object take(boolean priorityOnly) throws InterruptedException;

    /**
     * returns the number of normal operations pending.
     *
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for
     * monitoring purposes.
     *
     * @return the number of normal pending operations.
     */
    int normalSize();

    /**
     * returns the number of priority operations pending.
     *
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for
     * monitoring purposes.
     *
     * @return the number of priority pending operations.
     */
    int prioritySize();

    /**
     * Returns the total number of pending operations.
     *
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for
     * monitoring purposes.
     *
     * @return the total number of pending operations.
     */
    int size();
}
