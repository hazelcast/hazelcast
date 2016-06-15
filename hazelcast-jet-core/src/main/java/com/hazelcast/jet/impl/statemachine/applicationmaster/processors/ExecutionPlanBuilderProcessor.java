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

import com.hazelcast.core.IFunction;
import com.hazelcast.jet.config.JetApplicationConfig;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.DefaultJetTupleFactory;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.impl.container.DataChannel;
import com.hazelcast.jet.impl.container.DefaultDataChannel;
import com.hazelcast.jet.impl.container.DefaultProcessingContainer;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerStartRequest;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ExecutionPlanBuilderProcessor implements ContainerPayLoadProcessor<DAG> {
    private final NodeEngine nodeEngine;
    private final JetTupleFactory tupleFactory;
    private final ClassLoader applicationClassLoader;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;
    private final ILogger logger;

    private final IFunction<Vertex, ProcessingContainer> containerProcessingCreator =
            new IFunction<Vertex, ProcessingContainer>() {
                @Override
                public ProcessingContainer apply(Vertex vertex) {
                    ProcessorDescriptor descriptor = vertex.getDescriptor();
                    String className = descriptor.getContainerProcessorClazz();
                    Object[] args = descriptor.getArgs();

                    Supplier<ContainerProcessor> processorFactory = containerProcessorFactory(className, args);

                    ProcessingContainer container = new DefaultProcessingContainer(
                            vertex,
                            processorFactory,
                            nodeEngine,
                            applicationContext,
                            tupleFactory
                    );

                    applicationMaster.registerContainer(vertex, container);

                    return container;
                }
            };

    public ExecutionPlanBuilderProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.nodeEngine = applicationMaster.getNodeEngine();
        this.applicationContext = applicationMaster.getApplicationContext();
        this.applicationClassLoader = this.applicationContext.getLocalizationStorage().getClassLoader();
        this.tupleFactory = new DefaultJetTupleFactory();
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void process(DAG dag) throws Exception {
        checkNotNull(dag);


        logger.fine("Processing DAG " + dag.getName());

        //Process dag and container's chain building
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();

        Map<Vertex, ProcessingContainer> vertex2ContainerMap = new HashMap<Vertex, ProcessingContainer>(dag.getVertices().size());

        List<ProcessingContainer> roots = new ArrayList<ProcessingContainer>();

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();

            logger.fine("Processing vertex=" + vertex.getName() + " for DAG " + dag.getName());

            List<Edge> edges = vertex.getInputEdges();
            ProcessingContainer next = this.containerProcessingCreator.apply(vertex);

            logger.fine("Processed vertex=" + vertex.getName() + " for DAG " + dag.getName());

            vertex2ContainerMap.put(vertex, next);

            if (edges.size() == 0) {
                roots.add(next);
            } else {
                for (Edge edge : edges) {
                    join(vertex2ContainerMap.get(edge.getInputVertex()), edge, next);
                }
            }
        }

        logger.fine("Processed vertices for DAG " + dag.getName());

        JetApplicationConfig jetApplicationConfig = this.applicationContext.getJetApplicationConfig();
        long secondsToAwait =
                jetApplicationConfig.getJetSecondsToAwait();

        for (ProcessingContainer container : roots) {
            this.applicationMaster.addFollower(container);
        }

        this.applicationMaster.deployNetworkEngine();

        logger.fine("Deployed network engine for DAG " + dag.getName());

        for (ProcessingContainer container : this.applicationMaster.containers()) {
            container.handleContainerRequest(new ContainerStartRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    private ProcessingContainer join(ProcessingContainer container, Edge edge, ProcessingContainer nextContainer) {
        linkWithChannel(container, nextContainer, edge);
        return nextContainer;
    }

    private void linkWithChannel(ProcessingContainer container,
                                 ProcessingContainer next,
                                 Edge edge
    ) {
        if ((container != null) && (next != null)) {
            DataChannel channel = new DefaultDataChannel(container, next, edge);

            container.addFollower(next);
            next.addPredecessor(container);

            container.addOutputChannel(channel);
            next.addInputChannel(channel);
        }
    }

    @SuppressWarnings("unchecked")
    private Supplier<ContainerProcessor> containerProcessorFactory(String className, Object... args) {
        try {
            Constructor<ContainerProcessor> resultConstructor = getConstructor(className, args);
            return () -> {
                try {
                    return resultConstructor.newInstance(args);
                } catch (Exception e) {
                    throw JetUtil.reThrow(e);
                }
            };
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }

    private Constructor<ContainerProcessor> getConstructor(String className, Object[] args) throws ClassNotFoundException {
        Class<ContainerProcessor> clazz =
                (Class<ContainerProcessor>) Class.forName(
                        className,
                        true,
                        this.applicationClassLoader
                );

        int i = 0;
        Class[] argsClasses = new Class[args.length];

        for (Object obj : args) {
            if (obj != null) {
                argsClasses[i++] = obj.getClass();
            }
        }

        for (Constructor constructor : clazz.getConstructors()) {
            if (constructor.getParameterTypes().length == argsClasses.length) {
                boolean valid = true;
                Class[] parameterTypes = constructor.getParameterTypes();
                for (int idx = 0; idx < argsClasses.length; idx++) {
                    Class argsClass = argsClasses[idx];
                    if ((argsClass != null) && !parameterTypes[idx].isAssignableFrom(argsClass)) {
                        valid = false;
                        break;
                    }
                }

                if (valid) {
                    return (Constructor<ContainerProcessor>) constructor;
                }
            }
        }

        throw new IllegalStateException("No constructor with arguments"
                + Arrays.toString(argsClasses)
                + " className=" + className
        );
    }
}
