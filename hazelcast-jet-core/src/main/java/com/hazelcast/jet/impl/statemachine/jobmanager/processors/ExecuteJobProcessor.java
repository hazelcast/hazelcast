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

package com.hazelcast.jet.impl.statemachine.jobmanager.processors;

import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.ExecutorContext;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerPayloadProcessor;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerExecuteRequest;
import com.hazelcast.logging.ILogger;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExecuteJobProcessor implements VertexRunnerPayloadProcessor<Void> {
    private final long secondsToAwait;
    private final ExecutorContext executorContext;
    private final JobManager jobManager;
    private final JobContext jobContext;
    private final ILogger logger;

    public ExecuteJobProcessor(JobManager jobManager) {
        this.jobManager = jobManager;
        jobContext = jobManager.getJobContext();
        secondsToAwait = jobContext.getJobConfig().getSecondsToAwait();
        executorContext = jobContext.getExecutorContext();
        logger = jobContext.getNodeEngine().getLogger(ExecuteJobProcessor.class);
    }

    @Override
    public void process(Void payload) throws Exception {
        jobManager.registerExecution();

        startRunners();

        List<Task> networkTasks = jobContext.getExecutorContext().getNetworkTasks();
        List<Task> processingTasks = jobContext.getExecutorContext().getProcessingTasks();

        try {
            executorContext.getNetworkExecutor().submitTaskContext(networkTasks);
            executorContext.getProcessingExecutor().submitTaskContext(processingTasks);
        } catch (Throwable e) {
            try {
                for (Task networkTask : networkTasks) {
                    networkTask.interrupt(e);
                }
            } finally {
                for (Task processingTask : processingTasks) {
                    processingTask.interrupt(e);
                }
            }

            if (e instanceof Exception) {
                throw (Exception) e;
            } else {
                throw (Error) e;
            }
        }
    }

    private void startRunners() throws Exception {
        Iterator<Vertex> iterator = jobManager.getDag().getRevertedTopologicalVertexIterator();

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            VertexRunner vertexRunner = jobManager.getRunnerByVertex(vertex);
            vertexRunner.handleRequest(new VertexRunnerExecuteRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }
    }
}
