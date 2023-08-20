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
 * A {@link TaskQueue} can be configured with a {@link TaskRunner} which will process
 * every task issued to that TaskQueue.
 */
public interface TaskRunner {

    // Indicates that the task is yielding; so there is more work to do but the thread is willing to
    // give up the CPU to let other tasks run.
    int TASK_YIELD = 2;
    // Indicates that the task has completed and doesn't need to be reinserted into the {@link TaskQueue}.
    int TASK_COMPLETED = 0;
    // Indicates that the task is blocked and can be removed from the run queue of the scheduler.
    int TASK_BLOCKED = 1;

    /**
     * Initializes the TaskFactory with the given eventloop.
     *
     * @param eventloop the Eventloop this TaskFactory belongs to.
     */
    void init(Eventloop eventloop);

    /**
     * Process a single task.
     *
     * @param task the task.
     * @return the task state.
     */
    int run(Object task) throws Exception;
}
