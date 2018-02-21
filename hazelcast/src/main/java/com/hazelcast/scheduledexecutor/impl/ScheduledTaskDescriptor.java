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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metadata holder for scheduled tasks.
 * Active tasks, eg. not suspended, hold a non-null {@link #future} reference. Suspended ones, i.e., backups
 * or on-going migration have {@link #future} set to null.
 * <p>
 * For partition owned tasks, writes to the fields are done through the partition-thread.
 * For member owned tasks, writes to the fields are done through the generic-thread.
 * Reads on the fields, follow the same principal.
 */
public class ScheduledTaskDescriptor
        implements IdentifiedDataSerializable {

    private TaskDefinition definition;

    private transient ScheduledFuture<?> future;

    private final AtomicReference<ScheduledTaskResult> resultRef = new AtomicReference<ScheduledTaskResult>(null);

    private volatile ScheduledTaskStatisticsImpl stats;

    private volatile Map<?, ?> state;

    public ScheduledTaskDescriptor() {
    }

    public ScheduledTaskDescriptor(TaskDefinition definition) {
        this.definition = definition;
        this.state = new HashMap();
        this.stats = new ScheduledTaskStatisticsImpl();
    }

    public ScheduledTaskDescriptor(TaskDefinition definition, Map<?, ?> state, ScheduledTaskStatisticsImpl stats,
                                   ScheduledTaskResult result) {
        this.definition = definition;
        this.stats = stats;
        this.state = state;
        this.resultRef.set(result);
    }

    public TaskDefinition getDefinition() {
        return definition;
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

    /**
     * Suspended is a task that either has never been scheduled before (aka. backups) or it got suspended (aka. temporarily
     * stopped) during migration from one member to another.
     *
     * <p> When suspended, a task (if ever scheduled before), maintains its statistics and its actual runState,
     * however its associated {@link java.util.concurrent.Future} is cancelled and nullified. Upon future, rescheduling,
     * it will acquire be assigned on a different Future. <p> Task ownership is also restored to default,
     * which will be fixed when migrations finish and a new master is selected.
     */
    void suspend() {
        // Result is not set, allowing task to get re-scheduled, if/when needed.

        if (future != null) {
            this.future.cancel(true);
            this.future = null;
        }
    }

    boolean cancel(boolean mayInterrupt) {
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

    boolean isCancelled() {
        ScheduledTaskResult result = resultRef.get();
        boolean wasCancelled = result != null && result.wasCancelled();
        return wasCancelled || (future != null && future.isCancelled());
    }

    boolean isDone() {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduledTaskDescriptor that = (ScheduledTaskDescriptor) o;
        return (definition == that.definition) || (definition != null && definition.equals(that.definition));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new TaskDefinition[]{definition});
    }

    @Override
    public String toString() {
        return "ScheduledTaskDescriptor{"
                + "definition=" + definition
                + ", future=" + future
                + ", stats=" + stats
                + ", resultRef=" + resultRef.get()
                + ", state=" + state
                + '}';
    }

}
