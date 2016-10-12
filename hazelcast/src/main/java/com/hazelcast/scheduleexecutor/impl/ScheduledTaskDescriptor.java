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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by Thomas Kountis.
 */
public class ScheduledTaskDescriptor
        implements IdentifiedDataSerializable {

    private RunnableDefinition definition;

    private ScheduledFuture<?> scheduledFuture;

    private AmendableScheduledTaskStatistics stats;

    private Map state;

    public ScheduledTaskDescriptor() {
    }

    public ScheduledTaskDescriptor(RunnableDefinition definition, ScheduledFuture<?> scheduledFuture,
                                   Map taskState, AmendableScheduledTaskStatistics stats) {
        this.definition = definition;
        this.scheduledFuture = scheduledFuture;
        this.stats = stats;
        this.state = taskState;
    }

    public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    public RunnableDefinition getDefinition() {
        return definition;
    }

    public ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    public AmendableScheduledTaskStatistics getStats() {
        return stats;
    }

    public Map getState() {
        return state;
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
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        definition = in.readObject();
        state = in.readObject();
        stats = in.readObject();
    }
}
