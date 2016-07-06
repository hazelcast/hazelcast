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

package com.hazelcast.jet.impl.application;

import com.hazelcast.jet.impl.executor.ApplicationTaskContext;
import com.hazelcast.jet.impl.executor.SharedApplicationExecutor;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.executor.TaskExecutor;
import com.hazelcast.jet.impl.executor.DefaultApplicationTaskContext;
import com.hazelcast.jet.impl.executor.StateMachineTaskExecutorImpl;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;

/**
 * Abstract context which holds all executors of the application
 */
public class ExecutorContext {
    private final ApplicationTaskContext networkTaskContext;
    private final SharedApplicationExecutor networkExecutor;
    private final TaskExecutor containerStateMachineExecutor;
    private final SharedApplicationExecutor processingExecutor;
    private final TaskExecutor applicationStateMachineExecutor;
    private final ApplicationTaskContext applicationTaskContext;
    private final TaskExecutor applicationMasterStateMachineExecutor;

    public ExecutorContext(
            String name,
            ApplicationConfig applicationConfig,
            NodeEngine nodeEngine,
            SharedApplicationExecutor networkExecutor,
            SharedApplicationExecutor processingExecutor) {
        this.networkExecutor = networkExecutor;
        this.processingExecutor = processingExecutor;
        int awaitingTimeOut = applicationConfig.getSecondsToAwait();

        this.networkTaskContext = new DefaultApplicationTaskContext(new ArrayList<Task>());
        this.applicationTaskContext = new DefaultApplicationTaskContext(new ArrayList<Task>());

        this.containerStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-container-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.applicationStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-application-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.applicationMasterStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-application-master-state_machine", 1, awaitingTimeOut, nodeEngine);
    }

    /**
         * @return executor for application state-machine
         */
    public TaskExecutor getApplicationStateMachineExecutor() {
        return this.applicationStateMachineExecutor;
    }

    /**
         * @return executor for processing container state-machine
         */
    public TaskExecutor getDataContainerStateMachineExecutor() {
        return this.containerStateMachineExecutor;
    }

    /**
         * @return executor for application-master state-machine
         */
    public TaskExecutor getApplicationMasterStateMachineExecutor() {
        return this.applicationMasterStateMachineExecutor;
    }

    /**
         * @return shared executor to manage network specific tasks
         */
    public SharedApplicationExecutor getNetworkExecutor() {
        return this.networkExecutor;
    }

    /**
         * @return shared executor to manage processing specific tasks
         */
    public SharedApplicationExecutor getProcessingExecutor() {
        return this.processingExecutor;
    }

    /**
         * @return context for the network specific tasks
         */
    public ApplicationTaskContext getNetworkTaskContext() {
        return this.networkTaskContext;
    }

    /**
         * @return context for the application specific tasks
         */
    public ApplicationTaskContext getApplicationTaskContext() {
        return this.applicationTaskContext;
    }
}
