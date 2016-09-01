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

package com.hazelcast.jet.impl.job;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.executor.BalancedExecutor;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.spi.NodeEngine;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract context which holds all executors of the job
 */
public class ExecutorContext {
    private final List<Task> networkTasks;
    private final BalancedExecutor networkExecutor;
    private final StateMachineExecutor vertexManagerStateMachineExecutor;
    private final BalancedExecutor processingExecutor;
    private final StateMachineExecutor jobStateMachineExecutor;
    private final List<Task> processingTasks;
    private final StateMachineExecutor jobManagerStateMachineExecutor;

    public ExecutorContext(
            String name,
            JobConfig jobConfig,
            NodeEngine nodeEngine,
            BalancedExecutor networkExecutor,
            BalancedExecutor processingExecutor) {
        this.networkExecutor = networkExecutor;
        this.processingExecutor = processingExecutor;
        int awaitingTimeOut = jobConfig.getSecondsToAwait();

        this.networkTasks = new ArrayList<>();
        this.processingTasks = new ArrayList<>();

        this.vertexManagerStateMachineExecutor =
                new StateMachineExecutor(name + "-vertex-manager-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.jobStateMachineExecutor =
                new StateMachineExecutor(name + "-job-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.jobManagerStateMachineExecutor =
                new StateMachineExecutor(name + "-job-manager-state_machine", 1, awaitingTimeOut, nodeEngine);
    }

    /**
     * @return executor for job state-machine
     */
    public StateMachineExecutor getJobStateMachineExecutor() {
        return jobStateMachineExecutor;
    }

    /**
     * @return executor for vertex manager state-machine
     */
    public StateMachineExecutor getVertexManagerStateMachineExecutor() {
        return vertexManagerStateMachineExecutor;
    }

    /**
     * @return executor for job manager state-machine
     */
    public StateMachineExecutor getJobManagerStateMachineExecutor() {
        return jobManagerStateMachineExecutor;
    }

    /**
     * @return shared executor to manage network specific tasks
     */
    public BalancedExecutor getNetworkExecutor() {
        return networkExecutor;
    }

    /**
     * @return shared executor to manage processing specific tasks
     */
    public BalancedExecutor getProcessingExecutor() {
        return processingExecutor;
    }

    /**
     * @return context for the network specific tasks
     */
    public List<Task> getNetworkTasks() {
        return networkTasks;
    }

    /**
     * @return context for the job specific tasks
     */
    public List<Task> getProcessingTasks() {
        return processingTasks;
    }
}
