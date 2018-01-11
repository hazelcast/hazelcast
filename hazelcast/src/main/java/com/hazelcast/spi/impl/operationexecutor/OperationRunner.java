/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;

/**
 * The OperationRunner is responsible for the actual running of operations.
 * <p>
 * So the {@link OperationExecutor} is responsible for 'executing' them (so finding a thread to run on), the actual work is done
 * by the {@link OperationRunner}. This separation of concerns makes the code a lot simpler and makes it possible to swap parts
 * of the system.
 * <p>
 * Since HZ 3.5 there are multiple OperationRunner instances; each partition will have its own OperationRunner, but also
 * generic threads will have their own OperationRunners. Each OperationRunner exposes the Operation it is currently working
 * on and this makes it possible to hook on all kinds of additional functionality like detecting slow operations, sampling which
 * operations are executed most frequently, check if an operation is still running, etc etc.
 */
public abstract class OperationRunner {

    protected final int partitionId;
    protected volatile Object currentTask;

    private volatile Thread currentThread;

    public OperationRunner(int partitionId) {
        this.partitionId = partitionId;
    }

    public abstract long executedOperationsCount();

    public abstract void run(Packet packet) throws Exception;

    public abstract void run(Runnable task);

    public abstract void run(Operation task);

    /**
     * Returns the current task that is executing. This value could be null if no operation is executing.
     * <p>
     * Value could be stale as soon as it is returned.
     * <p>
     * This method is thread-safe; so the thread that executes a task will set/unset the current task,
     * any other thread in the system is allowed to read it.
     *
     * @return the current running task.
     */
    public final Object currentTask() {
        return currentTask;
    }

    /**
     * Sets the thread that is running this OperationRunner instance.
     * <p>
     * This method should only be called from the {@link OperationExecutor} since this component is responsible for
     * executing operations on an OperationRunner.
     *
     * @param currentThread the current Thread. Can be called with 'null', clearing the currentThread field.
     */
    public final void setCurrentThread(Thread currentThread) {
        this.currentThread = currentThread;
    }

    /**
     * Get the thread that is currently running this OperationRunner instance.
     * <p>
     * This value only has meaning when an Operation is running. It depends on the implementation if the field is unset
     * after an operation is executed or not. So it could be that a value is returned while no operation is running.
     * <p>
     * For example, the {@link OperationExecutorImpl} will never unset
     * this field since each OperationRunner is bound to a single OperationThread; so this field is initialized when the
     * OperationRunner is created.
     * <p>
     * The returned value could be null. When it is null, currently no thread is running this OperationRunner.
     * <p>
     * Recommended idiom for slow operation detection:
     * 1: First read the operation and store the reference.
     * 2: Then read the current thread and store the reference
     * 3: Later read the operation again. If the operation-instance is the same, it means that you have captured the right thread.
     * <p>
     * Then you use this Thread to create a stack trace. It can happen that the stracktrace doesn't reflect the call-state the
     * thread had when the slow operation was detected. This could be solved by rechecking the currentTask after you have detected
     * the slow operation. BUt don't create a stacktrace before you do the first recheck of the operation because otherwise it
     * will cause a lot of overhead.
     *
     * @return the Thread that currently is running this OperationRunner instance.
     */
    public final Thread currentThread() {
        return currentThread;
    }

    /**
     * Returns the partitionId this OperationRunner is responsible for. If the partition ID is smaller than 0,
     * it is either a generic or ad hoc OperationRunner.
     * <p>
     * The value will never change for this OperationRunner instance.
     *
     * @return the partition ID.
     */
    public final int getPartitionId() {
        return partitionId;
    }
}
