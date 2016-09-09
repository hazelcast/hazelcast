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
import cascading.tuple.Tuple;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class FlowNodeProcessor implements Processor<Pair<Tuple, Tuple>, Pair<Tuple, Tuple>> {

    private JetStreamGraph graph;
    private final FlowNode node;
    private final Map<String, Integer> sourceNameToOrdinal;
    private final Holder<OutputCollector<Pair<Tuple, Tuple>>> outputHolder = new Holder<>();

    public FlowNodeProcessor(FlowNode node, Map<String, Integer> sourceNameToOrdinal) {
        this.node = node;
        this.sourceNameToOrdinal = sourceNameToOrdinal;
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
    public boolean process(InputChunk<Pair<Tuple, Tuple>> input,
                           OutputCollector<Pair<Tuple, Tuple>> output,
                           String sourceName) throws Exception {
        outputHolder.set(output);
        ProcessorInputSource accumulatedSource = graph.getAccumulatedInputSources().get(sourceName);
        ProcessorInputSource inputSource =
                accumulatedSource != null ? accumulatedSource : graph.getStreamedInputSource();
        try {
            Integer ordinal = sourceNameToOrdinal.get(sourceName);
            inputSource.process(input.iterator(), ordinal);
        } catch (Throwable e) {
            throw unchecked(e);
        }
        return true;
    }

    @Override
    public boolean complete(OutputCollector<Pair<Tuple, Tuple>> output) throws Exception {
        graph.getStreamedInputSource().finalizeProcessor();
        graph.getAccumulatedInputSources().values().forEach(ProcessorInputSource::finalizeProcessor);
        return true;
    }
}
