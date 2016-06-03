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

package com.hazelcast.jet.impl.statemachine.container.requests;

import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.impl.container.ContainerRequest;

public class ContainerFinalizedRequest implements ContainerRequest<ProcessingContainerEvent, ProcessingContainer> {
    private final ProcessingContainer processingContainer;

    public ContainerFinalizedRequest(ProcessingContainer processingContainer) {
        this.processingContainer = processingContainer;
    }

    @Override
    public ProcessingContainerEvent getContainerEvent() {
        return ProcessingContainerEvent.FINALIZE;
    }

    @Override
    public ProcessingContainer getPayLoad() {
        return this.processingContainer;
    }
}
