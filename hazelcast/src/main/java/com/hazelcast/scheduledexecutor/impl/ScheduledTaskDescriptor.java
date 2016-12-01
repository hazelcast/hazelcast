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

    public enum Mode {
        IDLE,
        RUNNING,
        SAFEPOINT
    }

    private TaskDefinition definition;

    private ScheduledFuture<?> scheduledFuture;

    private ScheduledTaskStatisticsImpl stats;

    private volatile Map<?, ?> stateSnapshot;

    private final AtomicReference<Mode> mode = new AtomicReference<Mode>(Mode.IDLE);

    public ScheduledTaskDescriptor() {
    }

    public ScheduledTaskDescriptor(TaskDefinition definition) {
        this.definition = definition;
        this.stateSnapshot = new HashMap();
        this.stats = new ScheduledTaskStatisticsImpl();
    }
    public ScheduledTaskDescriptor(TaskDefinition definition,
                                   Map<?, ?> stateSnapshot, ScheduledTaskStatisticsImpl stats) {
        this.definition = definition;
        this.stats = stats;
        this.stateSnapshot = stateSnapshot;
    }

    public TaskDefinition getDefinition() {
        return definition;
    }

    public ScheduledTaskStatisticsImpl getStats() {
        return stats;
    }

    Map<?, ?> getStateSnapshot() {
        return stateSnapshot;
    }

    void setStateSnapshot(Map<?, ?> snapshot) {
        this.stateSnapshot = snapshot;
    }

    ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    void setScheduledFuture(ScheduledFuture<?> future) {
        this.scheduledFuture = future;
    }

    AtomicReference<Mode> getModeRef() {
        return mode;
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
        out.writeObject(stateSnapshot);
        out.writeObject(stats);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        definition = in.readObject();
        stateSnapshot = in.readObject();
        stats = in.readObject();
    }
}
