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

package com.hazelcast.jet.impl.statemachine.jobmanager.events;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerResponse;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerState;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerInterruptRequest;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.JetUtil.uncheckedGet;

public class InterruptEventHandler implements Consumer<Void> {
    private final int secondToAwait;
    private final JobManager jobManager;

    public InterruptEventHandler(JobManager jobManager) {
        this.jobManager = jobManager;
        secondToAwait = jobManager.getJobContext().getJobConfig().getSecondsToAwait();
    }

    @Override
    public void accept(Void payload) {
        JobManagerState currentState = jobManager.getStateMachine().currentState();
        if (currentState != JobManagerState.EXECUTING) {
            return;
        }
        jobManager.registerInterruption();

        for (VertexRunner runner : jobManager.runners()) {
            ICompletableFuture<VertexRunnerResponse> future = runner.handleRequest(new VertexRunnerInterruptRequest());
            uncheckedGet(future, secondToAwait, TimeUnit.SECONDS);
        }
    }
}
