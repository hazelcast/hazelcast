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

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ScheduledTaskDescriptor {

    private final TaskDefinition definition;

    private final ScheduledFuture<?> scheduledFuture;

    private final ScheduledTaskStatisticsImpl stats;

    private final AtomicReference<Map<?, ?>> state;

    private final AtomicBoolean lock = new AtomicBoolean();

    public ScheduledTaskDescriptor(TaskDefinition definition, ScheduledFuture<?> scheduledFuture,
                                   AtomicReference<Map<?, ?>> taskState, ScheduledTaskStatisticsImpl stats) {
        this.definition = definition;
        this.scheduledFuture = scheduledFuture;
        this.stats = stats;
        this.state = taskState;
    }

    public AtomicBoolean getLock() {
        return lock;
    }

    public TaskDefinition getDefinition() {
        return definition;
    }

    public ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    public ScheduledTaskStatisticsImpl getStats() {
        return stats;
    }

    public AtomicReference<Map<?, ?>> getState() {
        return state;
    }

}
