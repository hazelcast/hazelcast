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

import com.hazelcast.jet.api.Dummy;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.api.container.DataChannel;
import com.hazelcast.jet.api.container.ProcessingContainer;

import java.util.List;

public class ContainerExecutionCompletedProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final ProcessingContainer container;
    private final ApplicationContext applicationContext;

    public ContainerExecutionCompletedProcessor(ProcessingContainer container) {
        this.container = container;
        this.applicationContext = container.getApplicationContext();
    }

    //payload - completed container
    @Override
    public void process(Dummy payLoad) throws Exception {
        List<DataChannel> channels = this.container.getOutputChannels();

        if (channels.size() > 0) {
            // We don't use foreach to prevent iterator creation
            for (int idx = 0; idx < channels.size(); idx++) {
                channels.get(idx).close();
            }
        }

        this.applicationContext.getApplicationMaster().handleContainerCompleted();
    }
}
