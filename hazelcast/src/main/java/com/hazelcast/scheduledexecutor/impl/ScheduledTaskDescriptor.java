/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metadata holder for scheduled tasks.
 * Scheduled ones have a reference in their future in {@link #future}.
 * Stashed ones have this reference null.
 */
public class ScheduledTaskDescriptor implements IdentifiedDataSerializable {

    private TaskDefinition definition;

    private ScheduledFuture<?> future;

    /**
     * Only accessed through a member lock or partition threads
     * Used to identify which replica of the task is the owner, to only return that instance
     * when {@link GetAllScheduledOnMemberOperation} operation is triggered.
     * This flag is set to true only on initial scheduling of a task, and on after a promotion (stashed or migration),
     * in the latter case the other replicas get disposed.
     */
    private transient boolean isTaskOwner;

    /**
     * SPMC (see. Member owned tasks)
     */
    private volatile ScheduledTaskStatisticsImpl stats;

    /**
     * MPMC (Multiple Producers Multiple Concumers)
     * MP when cancelling, due to member owned tasks, all other writes are SP and through partition threads.
     * Reads are MP for member owned tasks.
     */
    private AtomicReference<ScheduledTaskResult> resultRef = new AtomicReference<ScheduledTaskResult>(null);

    /**
     * SPMC
     */
    private Map<?, ?> state;

    public ScheduledTaskDescriptor() {
    }

    public ScheduledTaskDescriptor(TaskDefinition definition) {
        this.definition = definition;
        this.state = new HashMap();
        this.stats = new ScheduledTaskStatisticsImpl();
    }

    public ScheduledTaskDescriptor(TaskDefinition definition,
                                   Map<?, ?> state, ScheduledTaskStatisticsImpl stats,
                                   ScheduledTaskResult result) {
        this.definition = definition;
        this.stats = stats;
        this.state = state;
        this.resultRef.set(result);
    }

    public TaskDefinition getDefinition() {
        return definition;
    }

    public boolean isTaskOwner() {
        return isTaskOwner;
    }

    void setTaskOwner(boolean taskOwner) {
        this.isTaskOwner = taskOwner;
    }

    ScheduledTaskStatisticsImpl getStatsSnapshot() {
        return stats.snapshot();
    }

    Map<?, ?> getState() {
        return state;
    }

    ScheduledTaskResult getTaskResult() {
        return resultRef.get();
    }

    void setStats(ScheduledTaskStatisticsImpl stats) {
        this.stats = stats;
    }

    void setState(Map<?, ?> snapshot) {
        this.state = snapshot;
    }

    ScheduledFuture<?> getScheduledFuture() {
        return future;
    }

    void setScheduledFuture(ScheduledFuture<?> future) {
        this.future = future;
    }

    void setTaskResult(ScheduledTaskResult result) {
        this.resultRef.set(result);
    }

    Object get()
            throws ExecutionException, InterruptedException {

        ScheduledTaskResult result = resultRef.get();
        if (result != null) {
            result.checkErroneousState();
            return result.getReturnValue();
        }

        return future.get();
    }

    void stopForMigration() {
        // Result is not set, allowing task to get re-scheduled, if/when needed.
        this.isTaskOwner = false;
        this.future.cancel(true);
        this.future = null;
    }

    boolean cancel(boolean mayInterrupt)
            throws ExecutionException, InterruptedException {
        if (!resultRef.compareAndSet(null, new ScheduledTaskResult(true)) || future == null) {
            return false;
        }

        return future.cancel(mayInterrupt);
    }

    long getDelay(TimeUnit unit) {
        boolean wasDoneOrCancelled = resultRef.get() != null;
        if (wasDoneOrCancelled) {
            return 0;
        }

        return future.getDelay(unit);
    }

    boolean isCancelled()
            throws ExecutionException, InterruptedException {
        ScheduledTaskResult result = resultRef.get();
        boolean wasCancelled = result != null && result.wasCancelled();
        return wasCancelled || (future != null && future.isCancelled());
    }

    boolean isDone()
            throws ExecutionException, InterruptedException {
        boolean wasDone = resultRef.get() != null;
        return wasDone || (future != null && future.isDone());
    }

    boolean shouldSchedule() {
        // Stashed tasks that never got scheduled, and weren't cancelled in-between
        return future == null && this.resultRef.get() == null;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.TASK_DESCRIPTOR;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(definition);
        out.writeObject(state);
        out.writeObject(stats);
        out.writeObject(resultRef.get());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        definition = in.readObject();
        state = in.readObject();
        stats = in.readObject();
        resultRef.set((ScheduledTaskResult) in.readObject());
    }

    @Override
    public String toString() {
        return "ScheduledTaskDescriptor{"
                + "definition=" + definition + ", "
                + "future=" + future + ", "
                + "stats=" + stats + ", "
                + "state=" + state + ", "
                + "isTaskOwner=" + isTaskOwner + ", "
                + "result=" + resultRef.get()
                + '}';
    }

}
