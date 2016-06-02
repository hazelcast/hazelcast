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

package com.hazelcast.jet.internal.impl.container.events;

import com.hazelcast.jet.internal.api.container.ContainerContext;
import com.hazelcast.jet.internal.api.container.ContainerTask;
import com.hazelcast.jet.internal.api.container.ProcessingContainer;
import com.hazelcast.jet.internal.api.container.events.EventProcessor;
import com.hazelcast.jet.internal.api.container.events.EventProcessorFactory;
import com.hazelcast.jet.internal.api.container.task.TaskEvent;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.internal.api.container.task.TaskEvent.TASK_EXECUTION_COMPLETED;
import static com.hazelcast.jet.internal.api.container.task.TaskEvent.TASK_EXECUTION_ERROR;
import static com.hazelcast.jet.internal.api.container.task.TaskEvent.TASK_READY_FOR_FINALIZATION;


public class DefaultEventProcessorFactory implements EventProcessorFactory {
    private final Map<TaskEvent, EventProcessor> processorMap =
            new IdentityHashMap<TaskEvent, EventProcessor>();

    public DefaultEventProcessorFactory(AtomicInteger completedTasks,
                                        AtomicInteger interruptedTasks,
                                        AtomicInteger readyForFinalizationTasksCounter,
                                        ContainerTask[] containerTasks,
                                        ContainerContext containerContext,
                                        ProcessingContainer processingContainer) {
        this.processorMap.put(TASK_EXECUTION_COMPLETED, new TaskEventCompletedProcessor(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                containerTasks,
                containerContext,
                processingContainer
        ));
        this.processorMap.put(TASK_EXECUTION_ERROR, new TaskEventExecutionErrorProcessor(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                containerTasks,
                containerContext,
                processingContainer
        ));
        this.processorMap.put(TASK_READY_FOR_FINALIZATION, new TaskEventFinalizationProcessor(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                containerTasks,
                containerContext,
                processingContainer
        ));
    }

    @Override
    public EventProcessor getEventProcessor(TaskEvent event) {
        EventProcessor processor = processorMap.get(event);

        if (processor == null) {
            throw new UnsupportedOperationException("Unsupported event: " + event);
        }

        return processor;
    }
}
