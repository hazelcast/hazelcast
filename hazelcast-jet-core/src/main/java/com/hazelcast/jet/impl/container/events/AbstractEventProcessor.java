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

package com.hazelcast.jet.impl.container.events;


import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.impl.container.task.ContainerTask;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.container.JobManager;
import com.hazelcast.jet.impl.container.ContainerContextImpl;
import com.hazelcast.jet.impl.container.ContainerListenerCaller;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.logging.ILogger;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class AbstractEventProcessor implements EventProcessor {
    protected final ILogger logger;
    protected final ProcessingContainer container;
    protected final AtomicInteger completedTasks;
    protected final AtomicInteger interruptedTasks;
    protected final ContainerTask[] containerTasks;
    protected final ContainerContextImpl containerContext;
    protected final JobContext jobContext;
    protected final AtomicInteger readyForFinalizationTasksCounter;
    protected final JobManager jobManager;


    protected AbstractEventProcessor(
            AtomicInteger completedTasks,
            AtomicInteger interruptedTasks,
            AtomicInteger readyForFinalizationTasksCounter,
            ContainerTask[] containerTasks,
            ContainerContextImpl containerContext,
            ProcessingContainer container
    ) {
        this.container = container;
        this.completedTasks = completedTasks;
        this.containerTasks = containerTasks;
        this.interruptedTasks = interruptedTasks;
        this.containerContext = containerContext;
        this.jobContext = containerContext.getJobContext();
        this.jobManager = jobContext.getJobManager();
        this.readyForFinalizationTasksCounter = readyForFinalizationTasksCounter;
        this.logger = this.jobContext.getNodeEngine().getLogger(getClass());
    }

    protected <P> void handleContainerRequest(StateMachineRequest<ProcessingContainerEvent, P> request) {
        this.container.handleContainerRequest(request);
    }

    protected <T extends Throwable> void invokeContainerListeners(
            ContainerListenerCaller invocator,
            T... error
    ) {
        List<ContainerListener> listeners =
                this.jobContext.
                        getContainerListeners().
                        get(this.containerContext.getVertex().getName());

        if (listeners != null) {
            for (ContainerListener listener : listeners) {
                try {
                    invocator.call(listener, error);
                } catch (Throwable e) {
                    this.logger.warning(e.getMessage(), e);
                } finally {
                    listeners.remove(listener);
                }
            }
        }
    }
}

