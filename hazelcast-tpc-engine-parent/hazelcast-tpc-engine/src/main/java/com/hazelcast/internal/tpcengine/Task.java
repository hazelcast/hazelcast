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

import static com.hazelcast.internal.tpcengine.TaskRunner.TASK_BLOCKED;
import static com.hazelcast.internal.tpcengine.TaskRunner.TASK_COMPLETED;
import static com.hazelcast.internal.tpcengine.TaskRunner.TASK_YIELD;

/**
 * A Task that gets executed on the {@link Eventloop}. When a task runs, it can
 * either:
 * <ol>
 *     <li>blocked: it is waiting for some kind of event to complete. It will
 *     not be placed back on the task queue.</li>
 *     <li>completed: the task is complete and will not be placed back on
 *     the task queue.</li>
 *     <li>yield: the task has more work to do, but it gives up its timeslice and
 *     will be put back in the task queue so that it can run again</li>
 * </ol>
 * <p/>
 * The reason that a Task is a runnable and the Eventloop doesn't directly execute
 * the Task is that we want to execute any Runnable on the Eventloop and we do not
 * want to wrap runnables in Task objects.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public abstract class Task implements Runnable {

    public TaskQueue taskQueue;

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    public abstract int process();

    @Override
    public final void run() {
        try {
            int status = process();
            switch (status) {
                case TASK_BLOCKED:
                    // when the task unblocks, it will add itself to its taskqueue
                    // and get the taskqueue scheduled.
                    break;
                case TASK_COMPLETED:
                    //task.release();
                    break;
                case TASK_YIELD:
                    // todo: we should check if there is a inside.
                    taskQueue.offerInside(this);
                    break;
                default:
                    throw new IllegalStateException("Unsupported status: " + status);
            }
        } catch (Exception e) {
            logger.warning(e);
        }
    }
}
