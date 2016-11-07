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


import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.ListenerCallable;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerEvent;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.runtime.VertexRunnerListener;
import com.hazelcast.logging.ILogger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class AbstractTaskEventHandler implements TaskEventHandler {
    protected final ILogger logger;
    protected final VertexRunner vertexRunner;
    protected final AtomicInteger completedTasks;
    protected final AtomicInteger interruptedTasks;
    protected final JobContext jobContext;
    protected final AtomicInteger readyForFinalizationTasksCounter;
    protected final JobManager jobManager;


    protected AbstractTaskEventHandler(
            AtomicInteger completedTasks,
            AtomicInteger interruptedTasks,
            AtomicInteger readyForFinalizationTasksCounter,
            VertexRunner vertexRunner
    ) {
        this.vertexRunner = vertexRunner;
        this.completedTasks = completedTasks;
        this.interruptedTasks = interruptedTasks;
        this.jobContext = vertexRunner.getJobContext();
        this.jobManager = jobContext.getJobManager();
        this.readyForFinalizationTasksCounter = readyForFinalizationTasksCounter;
        this.logger = this.jobContext.getNodeEngine().getLogger(getClass());
    }

    protected <P> void handleRequest(StateMachineRequest<VertexRunnerEvent, P> request) {
        vertexRunner.handleRequest(request);
    }

    protected <T extends Throwable> void invokeListeners(ListenerCallable callable, T... error) {
        String name = vertexRunner.getVertex().getName();
        List<VertexRunnerListener> listeners = jobContext.getVertexRunnerListeners().get(name);

        if (listeners != null) {
            for (VertexRunnerListener listener : listeners) {
                try {
                    callable.call(listener, error);
                } catch (Throwable e) {
                    logger.warning(e.getMessage(), e);
                } finally {
                    listeners.remove(listener);
                }
            }
        }
    }
}

