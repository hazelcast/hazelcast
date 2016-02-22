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

package com.hazelcast.jet.api.application;

import com.hazelcast.jet.api.executor.ApplicationTaskContext;
import com.hazelcast.jet.api.executor.SharedApplicationExecutor;
import com.hazelcast.jet.api.executor.TaskExecutor;

/**
 * Abstract context which holds all executors of the application
 */
public interface ExecutorContext {
    /**
     * @return executor for application state-machine;
     */
    TaskExecutor getApplicationStateMachineExecutor();

    /**
     * @return executor for processing container state-machine;
     */
    TaskExecutor getDataContainerStateMachineExecutor();

    /**
     * @return executor for application-master state-machine;
     */
    TaskExecutor getApplicationMasterStateMachineExecutor();

    /**
     * @return shared executor to manage network specific tasks;
     */
    SharedApplicationExecutor getNetworkExecutor();

    /**
     * @return shared executor to manage processing specific tasks;
     */
    SharedApplicationExecutor getProcessingExecutor();

    /**
     * @return context for the network specific tasks;
     */
    ApplicationTaskContext getNetworkTaskContext();

    /**
     * @return context for the application specific tasks;
     */
    ApplicationTaskContext getApplicationTaskContext();
}
