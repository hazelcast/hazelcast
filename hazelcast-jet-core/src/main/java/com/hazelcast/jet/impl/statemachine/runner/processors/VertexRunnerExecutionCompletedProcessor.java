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

package com.hazelcast.jet.impl.statemachine.runner.processors;

import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.DataChannel;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerPayloadProcessor;

import java.util.List;

public class VertexRunnerExecutionCompletedProcessor implements VertexRunnerPayloadProcessor<Void> {
    private final VertexRunner runner;
    private final JobContext jobContext;

    public VertexRunnerExecutionCompletedProcessor(VertexRunner runner) {
        this.runner = runner;
        this.jobContext = runner.getJobContext();
    }

    //payload - completed vertex runner
    @Override
    public void process(Void payload) throws Exception {
        List<DataChannel> channels = this.runner.getOutputChannels();

        if (channels.size() > 0) {
            // We don't use foreach to prevent iterator creation
            for (int idx = 0; idx < channels.size(); idx++) {
                channels.get(idx).close();
            }
        }

        this.jobContext.getJobManager().handleRunnerCompleted();
    }
}
