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

package com.hazelcast.jet.cascading.runtime;

import cascading.flow.FlowNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;

import java.util.List;
import java.util.Map;

public class FlowNodeProcessor implements Processor<Pair, Pair> {

    private JetStreamGraph graph;
    private final FlowNode node;
    private final Map<String, Integer> processOrdinalMap;
    private final Holder<OutputCollector<Pair>> outputHolder = new Holder<>();

    public FlowNodeProcessor(FlowNode node, Map<String, Integer> processOrdinalMap) {
        this.node = node;
        this.processOrdinalMap = processOrdinalMap;
    }

    @Override
    public void before(TaskContext context) {
        context.getSerializationOptimizer().addCustomType(TupleDataType.INSTANCE);
        HazelcastInstance instance = context.getJobContext().getNodeEngine().getHazelcastInstance();
        JetFlowProcess flowProcess = new JetFlowProcess(context.getJobContext().getJobConfig(), instance);
        flowProcess.setCurrentSliceNum(context.getTaskNumber());
        List<Edge> inputEdges = context.getVertex().getInputEdges();

        graph = new JetStreamGraph(flowProcess, node, outputHolder, inputEdges);
        graph.prepare();
        graph.getStreamedInputSource().beforeProcessing();
        for (ProcessorInputSource processorInputSource : graph.getAccumulatedInputSources().values()) {
            processorInputSource.beforeProcessing();
        }
    }

    @Override
    public boolean process(InputChunk<Pair> input,
                           OutputCollector<Pair> output,
                           String sourceName) throws Exception {
        outputHolder.set(output);
        ProcessorInputSource accumulatedSource = graph.getAccumulatedInputSources().get(sourceName);
        ProcessorInputSource inputSource = accumulatedSource == null ? graph.getStreamedInputSource()
                : accumulatedSource;
        try {
            Integer ordinal = processOrdinalMap.get(sourceName);
            inputSource.process(input.iterator(), ordinal);
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
        return true;
    }

    @Override
    public boolean complete(OutputCollector<Pair> output)
            throws Exception {
        graph.getStreamedInputSource().finalizeProcessor();
        for (ProcessorInputSource processorInputSource : graph.getAccumulatedInputSources().values()) {
            processorInputSource.finalizeProcessor();
        }
        return true;
    }
}
