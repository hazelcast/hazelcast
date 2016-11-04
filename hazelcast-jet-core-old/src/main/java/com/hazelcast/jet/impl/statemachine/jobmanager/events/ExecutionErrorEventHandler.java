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

import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.logging.ILogger;

import java.util.function.Consumer;

public class ExecutionErrorEventHandler implements Consumer<Throwable> {

    private final JobManager jobManager;
    private final ILogger logger;

    public ExecutionErrorEventHandler(JobManager jobManager) {
        logger = jobManager.getJobContext().getNodeEngine().getLogger(getClass());
        this.jobManager = jobManager;
    }

    public void accept(Throwable error) {
        if (error != null) {
            logger.severe(error.getMessage(), error);
        }

        for (VertexRunner runner : jobManager.runners()) {
            runner.interrupt(error);
        }
    }
}
