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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;

/**
 * The OperationHandler is responsible for the actual processing of operations.
 * <p/>
 * So the {@link com.hazelcast.spi.impl.OperationScheduler} is responsible for scheduling them (so finding a thread
 * to run on), the actual work is done by the {@link com.hazelcast.spi.impl.OperationHandler}.
 * <p/>
 * Since HZ 3.5 there are multiple OperationHandler instances; each partition will have its own OperationHandler, but also
 * generic threads will have their own OperationHandlers. Each OperationHandler exposes the Operation it is currently working
 * on and this makes it possible to hook on all kinds of additional functionality like detecting slow operations, sampling which
 * operations are executed most frequently, check if an operation is still running, etc etc.
 *
 * Why is this class an abstract class and not an interface?
 *
 * If this class would be an interface, the {@link #currentTask()} will be more expensive. THis method is going to be called
 * 'frequently'; if there are 5000 partitions, then there will be 5000 calls to this method. Since it is likely that there are
 * multiple implementations of this OperationHandler, there will be virtual method call that can't be inlined if this class
 * would be an interface.
 */
public abstract class OperationHandler {

    protected volatile Object currentTask;

    public abstract void process(Packet packet) throws Exception;

    public abstract void process(Runnable task);

    public abstract void process(Operation task);

    /**
     * Returns the current task that is executing. This value could be null if no operation is executing.
     * <p/>
     * Value could be stale as soon as it is returned.
     * <p/>
     * This method is thread-safe; so the thread that executes a task will set/unset the current task,
     * any other thread in the system is allowed to read it.
     *
     * @return the current running task.
     */
    public final Object currentTask() {
        return currentTask;
    }
}
