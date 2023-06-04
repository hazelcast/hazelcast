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

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

/**
 * A Task that gets executed on the {@link Eventloop}. A task can be blocked, completed or yield.
 * <p/>
 * The reason that a Task is a runnable and the Eventloop doesn't directly execute the Task is that
 * we want to execute any Runnable on the Eventloop and we do not want to wrap runnables in Task objects.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public abstract class Task implements Runnable {

    // Indicates that the task has completed and doesn't need to be reinserted into the {@link TaskQueue}.
    public static final int TASK_COMPLETED = 0;
    // Indicates that the task is blocked and can be removed from the run queue of the scheduler.
    public static final int TASK_BLOCKED = 1;
    // Indicates that the task is yielding; so there is more work to do but the thread is willing to
    // give up the CPU to let other tasks run.
    public static final int TASK_YIELD = 2;

    public TaskQueue taskQueue;

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    public abstract int process();

    @Override
    public final void run() {
        try {
            int status = process();
            switch (status) {
                case TASK_BLOCKED:
                    // the task is blocked, so we need to add it to the blocked queue.
                    break;
                case TASK_COMPLETED:
                    //task.release();
                    break;
                case TASK_YIELD:
                    // add it to the local
                    taskQueue.offerLocal(this);
                    break;
                default:
                    throw new IllegalStateException("Unsupported status: " + status);
            }
        } catch (Exception e) {
            logger.warning(e);
        }
    }
}
