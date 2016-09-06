/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cascading;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.io.Pair;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class JetConfigDefScheme extends TextLine {

    public JetConfigDefScheme(Fields sourceFields) {
        super(sourceFields);
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends JobConfig> flowProcess, Tap<JobConfig, Iterator<Pair>,
            OutputCollector<Pair>> tap, JobConfig conf) {
        // we should not see any config def values here
        if (flowProcess.getProperty("default") != null)
            throw new RuntimeException("default should be null");

        super.sourceConfInit(flowProcess, tap, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends JobConfig> flowProcess, Tap<JobConfig, Iterator<Pair>,
            OutputCollector<Pair>> tap, JobConfig conf) {
        // we should not see any config def values here
        if (flowProcess.getProperty("default") != null)
            throw new RuntimeException("default should be null");

        super.sinkConfInit(flowProcess, tap, conf);
    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JobConfig> flowProcess,
                              SourceCall<Void, Iterator<Pair>> sourceCall) throws IOException {
        if (!(flowProcess instanceof FlowProcessWrapper))
            throw new RuntimeException("not a flow process wrapper");

        if (!"process-default".equals(flowProcess.getProperty("default")))
            throw new RuntimeException("not default value");

        if (!"source-replace".equals(flowProcess.getProperty("replace")))
            throw new RuntimeException("not replaced value");

        flowProcess = ((FlowProcessWrapper) flowProcess).getDelegate();

        if (!"process-default".equals(flowProcess.getProperty("default")))
            throw new RuntimeException("not default value");

        if (!"process-replace".equals(flowProcess.getProperty("replace")))
            throw new RuntimeException("not replaced value");

        super.sourcePrepare(flowProcess, sourceCall);
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends JobConfig> flowProcess,
                            SinkCall<Integer, OutputCollector<Pair>> sinkCall) throws IOException {
        if (!(flowProcess instanceof FlowProcessWrapper))
            throw new RuntimeException("not a flow process wrapper");

        if (!"process-default".equals(flowProcess.getProperty("default")))
            throw new RuntimeException("not default value");

        if (!"sink-replace".equals(flowProcess.getProperty("replace")))
            throw new RuntimeException("not replaced value");

        flowProcess = ((FlowProcessWrapper) flowProcess).getDelegate();

        if (!"process-default".equals(flowProcess.getProperty("default")))
            throw new RuntimeException("not default value");

        if (!"process-replace".equals(flowProcess.getProperty("replace")))
            throw new RuntimeException("not replaced value");

        super.sinkPrepare(flowProcess, sinkCall);
    }
}
