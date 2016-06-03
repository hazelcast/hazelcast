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

package com.hazelcast.jet.impl.statemachine.applicationmaster.processors;

import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMaster;
import com.hazelcast.logging.ILogger;

public class ExecutionErrorProcessor implements ContainerPayLoadProcessor<Throwable> {

    private final ApplicationMaster applicationMaster;
    private final ILogger logger;

    public ExecutionErrorProcessor(ApplicationMaster applicationMaster) {
        this.logger = applicationMaster.getNodeEngine().getLogger(getClass());
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void process(Throwable error) throws Exception {
        if (error != null) {
            logger.severe(error.getMessage(), error);
        }

        for (ProcessingContainer container : this.applicationMaster.containers()) {
            container.interrupt(error);
        }
    }
}
