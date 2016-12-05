/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metadata holder for scheduled tasks.
 * Scheduled ones have a reference in their future in {@link #scheduledFuture}.
 * Stashed ones have this reference null.
 */
public class ScheduledTaskDescriptor implements IdentifiedDataSerializable {

    private TaskDefinition definition;

    private ScheduledFuture<?> scheduledFuture;

    /**
     * Stats are written from a single thread and read by many outside the partition threads. (see. Member owned tasks)
     */
    private AtomicReference<ScheduledTaskStatisticsImpl> stats = new AtomicReference<ScheduledTaskStatisticsImpl>(null);

    /**
     * State is only write/read only through the partition threads.
     */
    private Map<?, ?> state;

    public ScheduledTaskDescriptor() {
    }

    public ScheduledTaskDescriptor(TaskDefinition definition) {
        this.definition = definition;
        this.state = new HashMap();
        this.stats.set(new ScheduledTaskStatisticsImpl());
    }
    public ScheduledTaskDescriptor(TaskDefinition definition,
                                   Map<?, ?> state, ScheduledTaskStatisticsImpl stats) {
        this.definition = definition;
        this.stats.set(stats);
        this.state = state;
    }

    public TaskDefinition getDefinition() {
        return definition;
    }

    ScheduledTaskStatisticsImpl getStatsSnapshot() {
        return stats.get().snapshot();
    }

    void setStats(ScheduledTaskStatisticsImpl stats) {
        this.stats.set(stats);
    }

    Map<?, ?> getStateSnapshot() {
        return new HashMap(state);
    }

    void setState(Map<?, ?> snapshot) {
        this.state = snapshot;
    }

    ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    void setScheduledFuture(ScheduledFuture<?> future) {
        this.scheduledFuture = future;
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
        out.writeObject(stats.get());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        definition = in.readObject();
        state = in.readObject();
        stats.set((ScheduledTaskStatisticsImpl) in.readObject());
    }

    @Override
    public String toString() {
        return "ScheduledTaskDescriptor{" + "definition=" + definition + ", scheduledFuture=" + scheduledFuture + ", stats="
                + stats + ", state=" + state + '}';
    }

}
