/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

/**
 * Every Reactor has a scheduler. So incoming work (IOBuffers) can be scheduled
 * and it is up to the Scheduler to process these tasks. The Scheduler gets a frequent
 * tick from the {@link Reactor} to process whatever tasks are pending.
 * <p/>
 * It is important that the scheduler does a bit of work so that other sources
 * of work like e.g. networking, storage, outside tasks etc can be processed as well;
 * otherwise they would starve.
 * <p/>
 * This API probably needs a lot of additional design improvements. Currently there are
 * other sources of work for the eventloop (like concurrent tasks) that get processed
 * without any control of the scheduler. Perhaps it should all go through the scheduler
 * so that the scheduler controls all aspects of tasks executed on the Eventloop.
 */
public interface Scheduler {

    /**
     * Initializes the scheduler with the given eventloop.
     *
     * @param eventloop the Eventloop.
     */
    void init(Eventloop eventloop);

    /**
     * Gives the scheduler a tick. In this tick the scheduler can do a bit of work.
     *
     * @return true if there is more work, false otherwise.
     */
    boolean tick();

    /**
     * Schedules a task to be processed by this Scheduler.
     *
     * @param task the task.
     */
    void schedule(Object task);
}
