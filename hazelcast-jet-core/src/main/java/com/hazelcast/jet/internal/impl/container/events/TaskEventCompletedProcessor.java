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
import com.hazelcast.jet.internal.api.container.ContainerListenerCaller;
import com.hazelcast.jet.internal.api.container.ContainerTask;
import com.hazelcast.jet.internal.api.container.ProcessingContainer;
import com.hazelcast.jet.internal.api.container.task.TaskEvent;
import com.hazelcast.jet.internal.api.statemachine.container.ContainerRequest;
import com.hazelcast.jet.internal.api.statemachine.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.internal.impl.statemachine.container.requests.ContainerExecutionCompletedRequest;
import com.hazelcast.jet.internal.impl.statemachine.container.requests.ContainerInterruptedRequest;
import com.hazelcast.jet.api.container.ContainerListener;

import java.util.concurrent.atomic.AtomicInteger;

public class TaskEventCompletedProcessor extends AbstractEventProcessor {
    private volatile Throwable caughtError;

    protected TaskEventCompletedProcessor(AtomicInteger completedTasks,
                                          AtomicInteger interruptedTasks,
                                          AtomicInteger readyForFinalizationTasksCounter,
                                          ContainerTask[] containerTasks,
                                          ContainerContext containerContext,
                                          ProcessingContainer processingContainer) {
        super(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                containerTasks,
                containerContext,
                processingContainer
        );
    }

    public void process(ContainerTask containerTask,
                        TaskEvent event,
                        Throwable error) {
        if (error != null) {
            caughtError = error;
        }

        if (this.completedTasks.incrementAndGet() >= this.containerTasks.length) {
            this.completedTasks.set(0);

            if (caughtError == null) {
                handle(new ContainerExecutionCompletedRequest(), ContainerListener.EXECUTED_LISTENER_CALLER);
            } else {
                handle(new ContainerInterruptedRequest(caughtError), ContainerListener.INTERRUPTED_LISTENER_CALLER);
                caughtError = null;
            }
        }
    }

    private <P> void handle(ContainerRequest<ProcessingContainerEvent, P> request,
                            ContainerListenerCaller containerListenerCaller) {
        try {
            handleContainerRequest(request);
        } finally {
            invokeContainerListeners(containerListenerCaller);
        }
    }
}
