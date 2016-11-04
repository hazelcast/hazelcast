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

package com.hazelcast.jet.impl.runtime.events;

import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.TaskEvent;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.runtime.task.TaskEvent.TASK_EXECUTION_COMPLETED;
import static com.hazelcast.jet.impl.runtime.task.TaskEvent.TASK_EXECUTION_ERROR;
import static com.hazelcast.jet.impl.runtime.task.TaskEvent.TASK_READY_FOR_FINALIZATION;


public class TaskEventHandlerFactory {
    private final Map<TaskEvent, TaskEventHandler> handlerMap =
            new IdentityHashMap<>();

    public TaskEventHandlerFactory(VertexRunner vertexRunner) {
        AtomicInteger readyForFinalizationTasksCounter = new AtomicInteger(0);
        readyForFinalizationTasksCounter.set(vertexRunner.getVertexTasks().length);
        AtomicInteger completedTasks = new AtomicInteger(0);
        AtomicInteger interruptedTasks = new AtomicInteger(0);

        handlerMap.put(TASK_EXECUTION_COMPLETED, new CompletedEventHandler(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                vertexRunner
        ));
        handlerMap.put(TASK_EXECUTION_ERROR, new ExecutionErrorEventHandler(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                vertexRunner
        ));
        handlerMap.put(TASK_READY_FOR_FINALIZATION, new FinalizeEventHandler(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                vertexRunner
        ));
    }

    public TaskEventHandler getEventProcessor(TaskEvent event) {
        TaskEventHandler handler = handlerMap.get(event);

        if (handler == null) {
            throw new UnsupportedOperationException("Unsupported event: " + event);
        }

        return handler;
    }
}
