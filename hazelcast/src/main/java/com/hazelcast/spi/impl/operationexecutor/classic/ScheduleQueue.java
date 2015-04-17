/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.classic;

/**
 * The ScheduleQueue is a kind of priority queue where 'tasks' are queued for scheduling.
 * <p/>
 * ScheduleQueue support concurrent producers but only need to support single consumers.
 * <p/>
 * The ScheduledQueue also support priority tasks; so if a task with a priority comes in, than
 * that one is taken before any other normal operation is taken.
 * <p/>
 * The ordering between normal tasks will always be FIFO. And the same goes for the ordering between
 * priority tasks, but there is no ordering guarantee between priority and normal tasks.
 */
public interface ScheduleQueue {

    /**
     * Adds an task with normal priority to this queue.
     * <p/>
     * This method is thread safe.
     *
     * @param task     the item to add
     * @throws java.lang.NullPointerException if task is null
     */
    void add(Object task);

    /**
     * Adds an task with normal priority to this queue.
     * <p/>
     * This method is thread safe.
     *
     * @param task     the item to add
     * @throws java.lang.NullPointerException if task is null
     */
    void addUrgent(Object task);

    /**
     * Takes an item from this queue. If no item is available, the call blocks.
     * <p/>
     * This method should always be called by the same thread.
     *
     * @return the taken item.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    Object take() throws InterruptedException;

    /**
     * returns the number of normal operations pending.
     * <p/>
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for monitoring purposes.
     *
     * @return the number of normal pending operations.
     */
    int normalSize();

    /**
     * returns the number of priority operations pending.
     * <p/>
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for monitoring purposes.
     *
     * @return the number of priority pending operations.
     */
    int prioritySize();

    /**
     * Returns the total number of pending operations.
     * <p/>
     * This method is thread safe.
     *
     * This method returns a best effort value and should only be used for monitoring purposes.
     *
     * @return the total number of pending operations.
     */
    int size();
}
