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

package com.hazelcast.mapreduce.impl.task;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Schedules execution of a {@link Runnable} tasks. It guarantees only a single thread will ever execute
 * the task.
 *
 */
class ReducerTaskScheduler {
    private enum State {

        // Task is not running or requested for execution
        INACTIVE,

        // Task is currently being executed. No further execution was requested.
        RUNNING,

        // Task is running and another execution was requested
        REQUESTED
    }

    private final AtomicReference<State> state;
    private final ExecutorService executorService;
    private final Runnable task;

    ReducerTaskScheduler(ExecutorService executorService, Runnable task) {
        this.state = new AtomicReference<State>(State.INACTIVE);
        this.executorService = executorService;
        this.task = task;
    }

    private void scheduleExecution() {
        executorService.submit(task);
    }

    /**
     * Request a new task execution if needed.
     *
     */
    void requestExecution() {
        for (;;) {
            State currentState = state.get();
            switch (currentState) {
                case INACTIVE:
                    if (state.compareAndSet(State.INACTIVE, State.RUNNING)) {
                        scheduleExecution();
                        return;
                    }
                    break;
                case RUNNING:
                    if (state.compareAndSet(State.RUNNING, State.REQUESTED)) {
                        return;
                    }
                    break;
                default:
                    //someone else already requested execution
                    return;
            }
        }
    }

    /**
     * The task has to call this method after its execution.
     *
     */
    void afterExecution() {
        for (;;) {
            State currentState = state.get();
            switch (currentState) {
                case REQUESTED:
                    state.set(State.RUNNING);
                    scheduleExecution();
                    return;
                case RUNNING:
                    if (state.compareAndSet(State.RUNNING, State.INACTIVE)) {
                        return;
                    }
                    break;
                default:
                    throw new IllegalStateException("Inactive state is illegal here.");
            }
        }
    }
}
