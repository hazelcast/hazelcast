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

import com.hazelcast.internal.tpcengine.util.Reference;

/**
 * Every {@link TaskQueue} has a {@link TaskRunner} which will run tasks
 * issued to that TaskQueue.
 * <p/>
 * The TaskRunner also controls the error handling behavior using the
 * {@link #handleError(Object, Throwable)} method.
 */
public interface TaskRunner {

    /**
     * Initializes the TaskRunner with the given {@link Eventloop}.
     *
     * @param eventloop the Eventloop this TaskRunner belongs to.
     */
    void init(Eventloop eventloop);

    /**
     * Process a single task. It depends on the TaskRunner implementation which
     * type of tasks it can run.
     *
     * Instead of passing a task, a reference to the task is passed so that
     * the function can replace the object by something else. For example an
     * IOBuffer with a request is received, then the TaskRunner could decide
     * to deserialize the object and put it into a Task, and if that task yields,
     * this task is going to be scheduled and not the IOBuffer.
     *
     * @param the reference to the task.
     * @return the task state. See {@link Task}.
     */
    int run(Reference taskRef) throws Throwable;

    /**
     * Handles the throwable thrown by the {@link #run(Object)}. If you don't
     * want to handle the cause, rethrow it. But this will effectively mean that
     * the reactor is going to be terminated.
     *
     * @param cause
     * @return the run state of the task.
     */
    int handleError(Object task, Throwable cause);
}
