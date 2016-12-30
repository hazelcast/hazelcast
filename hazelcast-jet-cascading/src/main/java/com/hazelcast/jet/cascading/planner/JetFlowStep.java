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

package com.hazelcast.jet.cascading.planner;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessEdge;
import cascading.flow.stream.annotations.StreamMode;
import cascading.management.state.ClientState;
import cascading.pipe.Group;
import cascading.pipe.Splice;
import cascading.property.ConfigDef;
import cascading.tap.MultiSinkTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleHasher;
import cascading.util.Util;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.DefaultPartitionStrategy;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet.cascading.runtime.FlowNodeProcessor;
import com.hazelcast.jet.cascading.tap.InternalJetTap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class JetFlowStep extends BaseFlowStep<JetConfig> {

    private static final ILogger LOGGER = Logger.getLogger(JetFlowStep.class);

    public JetFlowStep(ElementGraph elementStepGraph,
                       FlowNodeGraph flowNodeGraph) {
        this(elementStepGraph, flowNodeGraph, null);

    }

    public JetFlowStep(ElementGraph elementStepGraph,
                       FlowNodeGraph flowNodeGraph,
                       Map<String, String> flowStepDescriptor) {
        super(elementStepGraph, flowNodeGraph, flowStepDescriptor);
    }

    @Override
    public JetConfig createInitializedConfig(FlowProcess<JetConfig> flowProcess,
                                             JetConfig parentConfig) {

        JetConfig currentConfig = parentConfig == null ? new JetConfig() : parentConfig;

        initTaps(flowProcess, currentConfig, getSourceTaps(), false);
        initTaps(flowProcess, currentConfig, getSinkTaps(), true);
        initTaps(flowProcess, currentConfig, getTraps(), true);

        initFromStepConfigDef(currentConfig);
        initFromNodeConfigDef(currentConfig);

//        for (String path : ((JetFlow) getFlow()).getClassPath()) {
//            currentConfig.addJar(path);
//        }
        return currentConfig;
    }

    @Override
    public void clean(JetConfig jetConfig) {
    }

    @Override
    public void logInfo(String message, Object... arguments) {
        LOGGER.info(formatMessage(message, arguments));
    }

    @Override
    public void logDebug(String message, Object... arguments) {
        LOGGER.fine(formatMessage(message, arguments));
    }

    @Override
    public void logWarn(String message) {
        LOGGER.warning(message);
    }

    @Override
    public void logWarn(String message, Throwable throwable) {
        LOGGER.warning(message, throwable);
    }

    @Override
    public void logWarn(String message, Object... arguments) {
        LOGGER.warning(formatMessage(message, arguments));
    }

    @Override
    public void logError(String message, Object... arguments) {
        LOGGER.severe(formatMessage(message, arguments));
    }

    @Override
    public void logError(String message, Throwable throwable) {
        LOGGER.severe(message, throwable);
    }

    @Override
    protected FlowStepJob createFlowStepJob(ClientState clientState,
                                            FlowProcess<JetConfig> flowProcess,
                                            JetConfig initializedStepConfig) {
        return new JetFlowStepJob((JetFlowProcess) flowProcess, buildDag(),
                clientState, initializedStepConfig, this);
    }

    protected DAG buildDag() {
        DAG dag = new DAG();
        FlowNodeGraph nodeGraph = getFlowNodeGraph();

        Map<String, AnnotatedVertex> vertexMap = new HashMap<>();
        Iterator<FlowNode> nodeIterator = nodeGraph.getOrderedTopologicalIterator();

        while (nodeIterator.hasNext()) {
            FlowNode node = nodeIterator.next();

            Set<? extends FlowElement> accumulatedSources = node.getSourceElements(StreamMode.Accumulated);

            Map<String, Map<Integer, Integer>> inputMap = new HashMap<>();
            Map<String, Set<Integer>> outputMap = new HashMap<>();

            Vertex vertex = new Vertex(node.getName(), new Supplier(node, getConfig().getProperties(),
                    inputMap, outputMap));
            dag.addVertex(vertex);

            AnnotatedVertex annotatedVertex = new AnnotatedVertex(vertex);
            annotatedVertex.inputMap = inputMap;
            annotatedVertex.outputMap = outputMap;
            vertexMap.put(node.getID(), annotatedVertex);

            for (FlowElement flowElement : node.getSourceElements()) {
                if (flowElement instanceof Tap) {
                    Tap tap = (Tap) flowElement;
                    String tapId = FlowElements.id(tap);
                    boolean isAccumulated = accumulatedSources.contains(flowElement);
                    Map<String, ProcessorMetaSupplier> suppliers = sourceTapToSuppliers(tap);
                    for (Map.Entry<String, ProcessorMetaSupplier> entry : suppliers.entrySet()) {
                        String id = entry.getKey();
                        AnnotatedVertex tapVertex = vertexMap.computeIfAbsent(id, k -> {
                            AnnotatedVertex v = new AnnotatedVertex(new Vertex(id, entry.getValue()));
                            dag.addVertex(v.vertex);
                            return v;
                        });
                        Edge edge = new Edge(tapVertex.vertex, tapVertex.currOutput++, vertex, annotatedVertex.currInput);
                        if (isAccumulated) {
                            edge = edge.broadcast().distributed().priority(0);
                        }
                        dag.addEdge(edge);

                        Map<Integer, Integer> ordinalMap = inputMap.computeIfAbsent(tapId, k -> new HashMap<>());
                        ordinalMap.put(annotatedVertex.currInput++, 0);
                        inputMap.put(id, ordinalMap);
                    }
                }
            }

            for (FlowElement flowElement : node.getSinkElements()) {
                if (flowElement instanceof Tap) {
                    Tap tap = (Tap) flowElement;
                    String id = FlowElements.id(flowElement);
                    Map<String, ProcessorMetaSupplier> suppliers = sinkTapToSuppliers(tap);
                    for (Map.Entry<String, ProcessorMetaSupplier> entry : suppliers.entrySet()) {
                        Vertex tapVertex = new Vertex(entry.getKey(), entry.getValue());
                        dag.addVertex(tapVertex);
                        Edge edge = new Edge(vertex, annotatedVertex.currOutput, tapVertex, 0);
                        dag.addEdge(edge);

                        Set<Integer> outputs = outputMap.computeIfAbsent(id, k -> new HashSet<>());
                        outputs.add(annotatedVertex.currOutput++);
                    }
                }
            }
        }

        for (ProcessEdge processEdge : nodeGraph.edgeSet()) {
            FlowNode sourceNode = nodeGraph.getEdgeSource(processEdge);
            FlowNode targetNode = nodeGraph.getEdgeTarget(processEdge);

            AnnotatedVertex sourceVertex = vertexMap.get(sourceNode.getID());
            AnnotatedVertex targetVertex = vertexMap.get(targetNode.getID());

            FlowElement element = processEdge.getFlowElement();
            String id = FlowElements.id(element);
            Set<? extends FlowElement> accumulatedSources = targetNode.getSourceElements(StreamMode.Accumulated);
            boolean isAccumulated = accumulatedSources.contains(element);

            Edge edge = new Edge(sourceVertex.vertex, sourceVertex.currOutput,
                    targetVertex.vertex, targetVertex.currInput)
                    .partitionedByCustom(getPartitioner(processEdge, targetNode, element))
                    .distributed();
            if (isAccumulated) {
                edge = edge.broadcast().priority(0);
            }

            Set<Integer> outputs = sourceVertex.outputMap.computeIfAbsent(id, k -> new HashSet<>());
            outputs.add(sourceVertex.currOutput++);

            Map<Integer, Integer> ordinalMap = targetVertex.inputMap.computeIfAbsent(id, k -> new HashMap<>());
            int ordinalAtTarget = (int) Util.getFirst(processEdge.getSourceProvidedOrdinals());
            ordinalMap.put(targetVertex.currInput++, ordinalAtTarget);
            dag.addEdge(edge);
        }
        return dag;
    }

    private Partitioner getPartitioner(ProcessEdge edge, FlowNode targetNode, FlowElement element) {
        if (element instanceof Group && element instanceof Splice) {
            Splice splice = (Splice) element;
            Map<String, Fields> keySelectors = splice.getKeySelectors();
            List<Scope> incomingScopes = new ArrayList<>(targetNode.getPreviousScopes(splice));
            int ordinal = Util.<Integer>getFirst(edge.getSourceProvidedOrdinals());
            for (Scope incomingScope : incomingScopes) {
                if (ordinal == incomingScope.getOrdinal()) {
                    Fields keyFields = keySelectors.get(incomingScope.getName());
                    Comparator[] merged = TupleHasher.merge(new Fields[]{keyFields});
                    if (!TupleHasher.isNull(merged)) {
                        return new TuplePartitioner(new TupleHasher(null, merged));
                    }
                    break;
                }
            }
        }
        return new DefaultPartitioner();
    }

    private static Object getKey(Object item) {
        return item instanceof Map.Entry ? ((Map.Entry) item).getKey() : item;
    }

    private Map<String, ProcessorMetaSupplier> sinkTapToSuppliers(Tap tap) {
        if (tap instanceof InternalJetTap) {
            InternalJetTap jetTap = (InternalJetTap) tap;
            return Collections.singletonMap(tap.getIdentifier(), jetTap.getSink());
        } else if (tap instanceof MultiSinkTap) {
            final Map<String, ProcessorMetaSupplier> sinks = new HashMap<>();
            Iterator childTaps = ((MultiSinkTap) tap).getChildTaps();
            childTaps.forEachRemaining(t -> sinks.putAll(sinkTapToSuppliers((Tap) t)));
            return sinks;
        }
        throw new UnsupportedOperationException("Unsupported sink tap: " + tap);
    }

    private Map<String, ProcessorMetaSupplier> sourceTapToSuppliers(Tap tap) {
        if (tap instanceof InternalJetTap) {
            InternalJetTap jetTap = (InternalJetTap) tap;
            return Collections.singletonMap(tap.getIdentifier(), jetTap.getSource());
        } else if (tap instanceof MultiSourceTap) {
            final Map<String, ProcessorMetaSupplier> sources = new HashMap<>();
            Iterator childTaps = ((MultiSourceTap) tap).getChildTaps();
            childTaps.forEachRemaining(t -> sources.putAll(sourceTapToSuppliers((Tap) t)));
            return sources;
        }

        throw new UnsupportedOperationException("Unsupported source tap: " + tap);
    }

    private String formatMessage(String message, Object[] arguments) {
        return MessageFormatter.arrayFormat(message, arguments).getMessage();
    }

    private void initTaps(FlowProcess<JetConfig> flowProcess, JetConfig config, Set<Tap> taps, boolean isSink) {
        if (!taps.isEmpty()) {
            for (Tap tap : taps) {
                if (isSink) {
                    tap.sinkConfInit(flowProcess, flowProcess.copyConfig(config));
                } else {
                    tap.sourceConfInit(flowProcess, flowProcess.copyConfig(config));
                }
            }
        }
    }

    private void initFromNodeConfigDef(final JetConfig jetConfig) {
        initConfFromNodeConfigDef(Util.getFirst(getFlowNodeGraph().vertexSet()).getElementGraph(),
                getSetterFor(jetConfig));
    }

    private void initFromStepConfigDef(final JetConfig jetConfig) {
        initConfFromStepConfigDef(getSetterFor(jetConfig));
    }

    private ConfigDef.Setter getSetterFor(final JetConfig jetConfig) {
        return new ConfigDef.Setter() {
            @Override
            public String set(String key, String value) {
                String oldValue = get(key);
                jetConfig.getProperties().setProperty(key, value);
                return oldValue;
            }

            @Override
            public String update(String key, String value) {
                String oldValue = get(key);
                if (oldValue == null) {
                    jetConfig.getProperties().setProperty(key, value);
                } else if (!oldValue.contains(value)) {
                    jetConfig.getProperties().setProperty(key, oldValue + "," + value);
                }
                return oldValue;
            }

            @Override
            public String get(String key) {
                String value = (String) jetConfig.getProperties().get(key);
                if (value == null || value.isEmpty()) {
                    return null;
                }
                return value;
            }
        };
    }

    private static final class AnnotatedVertex {
        private Vertex vertex;
        // current input ordinal
        private int currInput;
        // current output ordinal
        private int currOutput;

        // element id  -> jet ordinal -> cascading ordinal
        private Map<String, Map<Integer, Integer>> inputMap;

        // element id  -> jet ordinal
        private Map<String, Set<Integer>> outputMap;

        private AnnotatedVertex(Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public String toString() {
            return "VertexWithOrdinalInfo{" +
                    "vertex=" + vertex +
                    ", lastInputOrdinal=" + currInput +
                    ", lastOutputOrdinal=" + currOutput +
                    '}';
        }
    }

    private static final class TuplePartitioner implements Partitioner {

        private final TupleHasher hasher;

        private TuplePartitioner(TupleHasher hasher) {
            this.hasher = hasher;
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return Math.abs(hasher.hashCode((Tuple) getKey(item))) % partitionCount;
        }
    }

    private static class DefaultPartitioner implements Partitioner {

        private static final long serialVersionUID = 1L;

        private transient DefaultPartitionStrategy lookup;

        @Override
        public void init(DefaultPartitionStrategy lookup) {
            this.lookup = lookup;
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return lookup.getPartition(getKey(item));
        }
    }

    private static final class Supplier implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final FlowNode node;
        private final Properties properties;
        private final Map<String, Map<Integer, Integer>> inputMap;
        private final Map<String, Set<Integer>> outputMap;

        private transient JetInstance instance;

        private Supplier(FlowNode node, Properties properties, Map<String, Map<Integer, Integer>> inputMap,
                         Map<String, Set<Integer>> outputMap) {
            this.node = node;
            this.properties = properties;
            this.inputMap = inputMap;
            this.outputMap = outputMap;
        }

        @Override
        public void init(Context context) {
            instance = context.getJetInstance();
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(() -> new FlowNodeProcessor(instance, properties, node, inputMap, outputMap))
                         .limit(count).collect(toList());
        }
    }
}
