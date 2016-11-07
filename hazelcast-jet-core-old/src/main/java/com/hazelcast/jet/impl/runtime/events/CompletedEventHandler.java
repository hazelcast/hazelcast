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

import com.hazelcast.jet.runtime.VertexRunnerListener;
import com.hazelcast.jet.impl.runtime.ListenerCallable;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerEvent;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.runtime.task.TaskEvent;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerInterruptedRequest;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletedEventHandler extends AbstractTaskEventHandler {
    private volatile Throwable caughtError;

    protected CompletedEventHandler(AtomicInteger completedTasks,
                                    AtomicInteger interruptedTasks,
                                    AtomicInteger readyForFinalizationTasksCounter,
                                    VertexRunner vertexRunner) {
        super(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                vertexRunner
        );
    }

    public void handle(VertexTask vertexTask,
                       TaskEvent event,
                       Throwable error) {
        if (error != null) {
            caughtError = error;
        }

        if (this.completedTasks.incrementAndGet() >= vertexRunner.getVertexTasks().length) {
            this.completedTasks.set(0);

            if (caughtError == null) {
                handle(new VertexRunnerExecutionCompletedRequest(), VertexRunnerListener.EXECUTED_LISTENER_CALLER);
            } else {
                handle(new VertexRunnerInterruptedRequest(caughtError), VertexRunnerListener.INTERRUPTED_LISTENER_CALLER);
                caughtError = null;
            }
        }
    }

    private <P> void handle(StateMachineRequest<VertexRunnerEvent, P> request,
                            ListenerCallable listenerCallable) {
        try {
            handleRequest(request);
        } finally {
            invokeListeners(listenerCallable);
        }
    }
}
