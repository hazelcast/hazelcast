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

package com.hazelcast.jet.api.container.task;

import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ContainerProcessor;

/**
 * Interface which represents factory to create tasks' execution processors;
 *
 * Each processor has following structure:
 *
 * Producers --&gt; Processor -&gt; Consumers;
 */
public interface TaskProcessorFactory {
    /**
     * Construct processors in case when there is no producers or consumers;
     *
     * @param processor        - user-level container-processor;
     * @param containerContext - JET-container context;
     * @param processorContext - user-level processor context;
     * @param vertex           - corresponding vertex of DAG;
     * @param taskID           - id of task;
     * @return - task processor;
     */
    TaskProcessor simpleTaskProcessor(ContainerProcessor processor,
                                      ContainerContext containerContext,
                                      ProcessorContext processorContext, Vertex vertex,
                                      int taskID);

    /**
     * Construct processors in case when there are just consumers without producers;
     *
     * @param consumers        - list of output consumers;
     * @param processor        - user-level container processor;
     * @param containerContext - container context;
     * @param processorContext - user-level processor context;
     * @param vertex           - corresponding vertex of DAG;
     * @param taskID           - id of task;
     * @return - task processor;
     */
    TaskProcessor consumerTaskProcessor(ObjectConsumer[] consumers,
                                        ContainerProcessor processor,
                                        ContainerContext containerContext,
                                        ProcessorContext processorContext, Vertex vertex,
                                        int taskID);

    /**
     * Construct processors in case when there are just producers without consumers;
     *
     * @param producers        -   list of input  producers;
     * @param processor        -   user-level container processor;
     * @param containerContext -   container context;
     * @param processorContext -   user-level processor context;
     * @param vertex           -   corresponding vertex of DAG;
     * @param taskID           -   id of task;
     * @return - task processor;
     */
    TaskProcessor producerTaskProcessor(ObjectProducer[] producers,
                                        ContainerProcessor processor,
                                        ContainerContext containerContext,
                                        ProcessorContext processorContext, Vertex vertex,
                                        int taskID);

    /**
     * @param producers        - list of the input producers;
     * @param consumers        - list of the output consumers;
     * @param processor        - user-level container processor;
     * @param containerContext - container context;
     * @param processorContext - user-level processor context;
     * @param vertex           - corresponding vertex of DAG;
     * @param taskID           - id of task;
     * @return - task processor;
     */
    TaskProcessor actorTaskProcessor(ObjectProducer[] producers,
                                     ObjectConsumer[] consumers,
                                     ContainerProcessor processor,
                                     ContainerContext containerContext,
                                     ProcessorContext processorContext, Vertex vertex,
                                     int taskID);

    /**
     * Determines type of processor (empty, producer-only, consumer-only, producer-consumer);
     *
     * @param producers        - list of input producers;
     * @param consumers        - list of output consumers;
     * @param containerContext - container context;
     * @param processorContext - user-level processor context;
     * @param processor        - user-level container processor;
     * @param vertex           - corresponding vertex of DAG;
     * @param taskID           - id of task;
     * @return - task processor;
     */
    TaskProcessor getTaskProcessor(ObjectProducer[] producers,
                                   ObjectConsumer[] consumers,
                                   ContainerContext containerContext,
                                   ProcessorContext processorContext,
                                   ContainerProcessor processor,
                                   Vertex vertex,
                                   int taskID);
}
