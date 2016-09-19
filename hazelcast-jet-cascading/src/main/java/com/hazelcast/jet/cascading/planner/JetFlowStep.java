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
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.cascading.JetFlow;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet.cascading.runtime.FlowNodeProcessor;
import com.hazelcast.jet.cascading.runtime.IdentityProcessor;
import com.hazelcast.jet.cascading.tap.InternalJetTap;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.strategy.AllMembersDistributionStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.UuidUtil;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JetFlowStep extends BaseFlowStep<JobConfig> {

    public static final int PARALLELISM = Runtime.getRuntime().availableProcessors();
    //    public static final int PARALLELISM = 1;
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
    public JobConfig createInitializedConfig(FlowProcess<JobConfig> flowProcess,
                                             JobConfig parentConfig) {

        JobConfig currentConfig = parentConfig == null ? new JobConfig() : parentConfig;

        initTaps(flowProcess, currentConfig, getSourceTaps(), false);
        initTaps(flowProcess, currentConfig, getSinkTaps(), true);
        initTaps(flowProcess, currentConfig, getTraps(), true);

        initFromStepConfigDef(currentConfig);
        initFromNodeConfigDef(currentConfig);

        for (String path : ((JetFlow) getFlow()).getClassPath()) {
            currentConfig.addJar(path);
        }
        return currentConfig;
    }

    @Override
    public void clean(JobConfig jetFlowConfig) {
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
                                            FlowProcess<JobConfig> flowProcess,
                                            JobConfig initializedStepConfig) {
        return new JetFlowStepJob((JetFlowProcess) flowProcess, buildDag(),
                clientState, initializedStepConfig, this);
    }

    protected DAG buildDag() {
        DAG dag = new DAG();
        FlowNodeGraph nodeGraph = getFlowNodeGraph();

        Map<FlowNode, Vertex> vertexMap = new HashMap<>();
        Iterator<FlowNode> nodeIterator = nodeGraph.getOrderedTopologicalIterator();
        while (nodeIterator.hasNext()) {
            FlowNode node = nodeIterator.next();

            Map<String, Integer> processOrdinalMap = buildProcessOrdinalMap(node);
            Vertex vertex = new Vertex(node.getID(), FlowNodeProcessor.class, node, processOrdinalMap)
                    .parallelism(PARALLELISM);
            dag.addVertex(vertex);

            Set<? extends FlowElement> accumulatedSources = node.getSourceElements(StreamMode.Accumulated);
            addAccumulatedTaps(dag, vertex, accumulatedSources);
            Set<FlowElement> sources = new HashSet<>(node.getSourceTaps());
            sources.removeAll(accumulatedSources);

            if (node.getSinkElements().size() > 1) {
                throw new UnsupportedOperationException("Splits are not supported currently in Jet");
            }

            for (FlowElement element : sources) {
                if (element instanceof Tap) {
                    Tap tap = (Tap) element;
                    for (Source source : mapToSources(tap)) {
                        vertex.addSource(source);
                    }

                }
            }


            for (Tap tap : node.getSinkTaps()) {
                for (Sink sink : mapToSinks(tap)) {
                    vertex.addSink(sink);
                }
            }

            vertexMap.put(node, vertex);
        }

        processEdges(dag, nodeGraph, vertexMap);
        return dag;
    }

    private Map<String, Integer> buildProcessOrdinalMap(FlowNode node) {
        Set<ProcessEdge> inputs = getFlowNodeGraph().incomingEdgesOf(node);
        Map<String, Integer> processOrdinalMap = new HashMap<>();
        for (ProcessEdge input : inputs) {
            Set<Integer> sourceProvidedOrdinals = input.getSourceProvidedOrdinals();
            if (sourceProvidedOrdinals.size() != 1) {
                throw new JetException("Source can't provide more than one path");
            }
            Integer ordinal = sourceProvidedOrdinals.iterator().next();
            processOrdinalMap.put(input.getSourceProcessID(), ordinal);
        }
        return processOrdinalMap;
    }

    private void processEdges(DAG dag, FlowNodeGraph nodeGraph, Map<FlowNode, Vertex> vertexMap) {
        for (ProcessEdge edge : nodeGraph.edgeSet()) {
            FlowNode sourceNode = nodeGraph.getEdgeSource(edge);
            FlowNode targetNode = nodeGraph.getEdgeTarget(edge);

            Vertex sourceVertex = vertexMap.get(sourceNode);
            Vertex targetVertex = vertexMap.get(targetNode);

            FlowElement element = edge.getFlowElement();
            Set<? extends FlowElement> accumulatedSources
                    = targetNode.getSourceElements(StreamMode.Accumulated);

            if (accumulatedSources.contains(element)) {
                dag.addEdge(new Edge(edge.getFlowElementID(), sourceVertex, targetVertex)
                        .broadcast()
                        .distributed(AllMembersDistributionStrategy.INSTANCE)
                );
            } else {
                HashingStrategy hashingStrategy = getHashingStrategy(edge, targetNode, element);
                dag.addEdge(new Edge(edge.getID(), sourceVertex, targetVertex)
                        .partitioned(hashingStrategy)
                        .distributed());
            }
        }
    }

    private HashingStrategy getHashingStrategy(ProcessEdge edge, FlowNode targetNode, FlowElement element) {
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
                        return new TupleHashingStrategy(new TupleHasher(null, merged));
                    }
                    break;
                }
            }
        }
        return SerializedHashingStrategy.INSTANCE;
    }

    private void addAccumulatedTaps(DAG dag, Vertex vertex, Set<? extends FlowElement> accumulatedSources) {
        for (FlowElement element : accumulatedSources) {
            if (element instanceof Tap) {

                // for accumulated sources, we want everything broadcast to all the tasks
                Tap tap = (Tap) element;
                Vertex tapVertex = new Vertex(UuidUtil.newUnsecureUuidString(), IdentityProcessor.class)
                        .parallelism(PARALLELISM);
                for (Source source : mapToSources(tap)) {
                    tapVertex.addSource(source);
                }
                dag.addVertex(tapVertex);
                Edge edge = new Edge(FlowElements.id(element), tapVertex, vertex)
                        .broadcast()
                        .distributed(AllMembersDistributionStrategy.INSTANCE);
                dag.addEdge(edge);
            }
        }
    }

    private Collection<Sink> mapToSinks(Tap tap) {
        if (tap instanceof InternalJetTap) {
            return Collections.singletonList(((InternalJetTap) tap).getSink());
        } else if (tap instanceof MultiSinkTap) {
            final List<Sink> sinks = new ArrayList<>();
            Iterator childTaps = ((MultiSinkTap) tap).getChildTaps();
            childTaps.forEachRemaining(t -> sinks.addAll(mapToSinks((Tap) t)));
            return sinks;
        }

        throw new UnsupportedOperationException("Unsupported sink tap: " + tap);
    }

    private Collection<Source> mapToSources(Tap tap) {
        if (tap instanceof InternalJetTap) {
            Source source = ((InternalJetTap) tap).getSource();
            return Collections.singletonList(source);
        } else if (tap instanceof MultiSourceTap) {
            final List<Source> sources = new ArrayList<>();
            Iterator childTaps = ((MultiSourceTap) tap).getChildTaps();
            childTaps.forEachRemaining(t -> sources.addAll(mapToSources((Tap) t)));
            return sources;
        }

        throw new UnsupportedOperationException("Unsupported source tap: " + tap);
    }

    private String formatMessage(String message, Object[] arguments) {
        return MessageFormatter.arrayFormat(message, arguments).getMessage();
    }

    private void initTaps(FlowProcess<JobConfig> flowProcess, JobConfig config, Set<Tap> taps, boolean isSink) {
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

    private void initFromNodeConfigDef(final JobConfig jobConfig) {
        initConfFromNodeConfigDef(Util.getFirst(getFlowNodeGraph().vertexSet()).getElementGraph(),
                getSetterFor(jobConfig));
    }

    private void initFromStepConfigDef(final JobConfig jobConfig) {
        initConfFromStepConfigDef(getSetterFor(jobConfig));
    }

    private ConfigDef.Setter getSetterFor(final JobConfig jobConfig) {
        return new ConfigDef.Setter() {
            @Override
            public String set(String key, String value) {
                String oldValue = get(key);
                jobConfig.getProperties().setProperty(key, value);
                return oldValue;
            }

            @Override
            public String update(String key, String value) {
                String oldValue = get(key);
                if (oldValue == null) {
                    jobConfig.getProperties().setProperty(key, value);
                } else if (!oldValue.contains(value)) {
                    jobConfig.getProperties().setProperty(key, oldValue + "," + value);
                }
                return oldValue;
            }

            @Override
            public String get(String key) {
                String value = (String) jobConfig.getProperties().get(key);
                if (value == null || value.isEmpty()) {
                    return null;
                }
                return value;
            }
        };
    }

    private static class TupleHashingStrategy implements HashingStrategy<Pair<Tuple, Tuple>, Tuple> {

        private final TupleHasher hasher;

        public TupleHashingStrategy(TupleHasher hasher) {
            this.hasher = hasher;
        }

        @Override
        public int hash(Pair<Tuple, Tuple> tuple, Tuple partitionKey,
                        JobContext jobContext) {
            return hasher.hashCode(partitionKey);
        }
    }
}
