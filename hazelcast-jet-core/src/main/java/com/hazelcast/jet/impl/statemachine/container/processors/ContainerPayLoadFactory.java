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

package com.hazelcast.jet.impl.statemachine.container.processors;

import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.impl.container.ContainerPayLoadProcessor;

public final class ContainerPayLoadFactory {
    private ContainerPayLoadFactory() {
    }

    public static ContainerPayLoadProcessor getProcessor(ProcessingContainerEvent event,
                                                         ProcessingContainer container) {
        switch (event) {
            case START:
                return new ContainerStartProcessor(container);
            case EXECUTE:
                return new ContainerExecuteProcessor();
            case INTERRUPT:
                return new ContainerInterruptProcessor(container);
            case INTERRUPTED:
                return new ContainerInterruptedProcessor(container);
            case EXECUTION_COMPLETED:
                return new ContainerExecutionCompletedProcessor(container);
            default:
                return null;
        }
    }
}
