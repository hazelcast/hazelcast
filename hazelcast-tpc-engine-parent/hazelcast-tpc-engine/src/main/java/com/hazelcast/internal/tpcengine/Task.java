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
 * A Task that gets executed on the {@link Eventloop}. When a task runs, it can
 * either:
 * <ol>
 *     <li>
 *         RUN_BLOCKED: it is waiting for some kind of event to complete. It will
 *         not be placed back on the task queue.
 *     </li>
 *     <li>
 *         RUN_COMPLETED: the task is complete and will not be placed back on
 *         the task queue.
 *     </li>
 *     <li>
 *         RUN_YIELD: the task has more work to do, but it gives up its timeslice
 *         and will be put back in the task queue so that it can run again.
 *     </li>
 * </ol>
 */
public abstract class Task {

    // Indicates that the task is yielding; so there is more work to do but
    // the thread is willing to give up the CPU to let other tasks run.
    public static final int RUN_YIELD = 2;
    // Indicates that the task has completed and doesn't need to be
    // reinserted into the {@link TaskQueue}.
    public static final int RUN_COMPLETED = 0;
    // Indicates that the task is blocked and doesn't need to be added back
    // back to the TaskQueue. The task will schedule itself back on the
    // TaskQueue when it gets unblocked.
    public static final int RUN_BLOCKED = 1;

    /**
     * Runs the task
     *
     * @return on of the above run states.
     * @throws Throwable if anything goes wrong while processing this task.
     */
    public abstract int run() throws Throwable;
}
