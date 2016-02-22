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

package com.hazelcast.jet.impl.container.task.processors.factory;

import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.api.container.task.TaskProcessorFactory;
import com.hazelcast.jet.impl.container.task.processors.ActorTaskProcessor;
import com.hazelcast.jet.impl.container.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.container.task.processors.ProducerTaskProcessor;
import com.hazelcast.jet.impl.container.task.processors.SimpleTaskProcessor;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTaskProcessorFactory implements TaskProcessorFactory {
    @Override
    public TaskProcessor simpleTaskProcessor(ContainerProcessor processor,
                                             ContainerContext containerContext,
                                             ProcessorContext processorContext,
                                             Vertex vertex,
                                             int taskID) {
        return new SimpleTaskProcessor(processor, containerContext, processorContext);
    }

    @Override
    public TaskProcessor consumerTaskProcessor(ObjectConsumer[] consumers,
                                               ContainerProcessor processor,
                                               ContainerContext containerContext,
                                               ProcessorContext processorContext,
                                               Vertex vertex,
                                               int taskID) {
        return new ConsumerTaskProcessor(consumers, processor, containerContext, processorContext);
    }

    @Override
    public TaskProcessor producerTaskProcessor(ObjectProducer[] producers,
                                               ContainerProcessor processor,
                                               ContainerContext containerContext,
                                               ProcessorContext processorContext,
                                               Vertex vertex,
                                               int taskID) {
        return new ProducerTaskProcessor(producers, processor, containerContext, processorContext, taskID);
    }

    @Override
    public TaskProcessor actorTaskProcessor(ObjectProducer[] producers,
                                            ObjectConsumer[] consumers,
                                            ContainerProcessor processor,
                                            ContainerContext containerContext,
                                            ProcessorContext processorContext,
                                            Vertex vertex,
                                            int taskID) {
        return new ActorTaskProcessor(
                producers,
                processor,
                containerContext,
                processorContext,
                consumerTaskProcessor(consumers, processor, containerContext, processorContext, vertex, taskID),
                taskID
        );
    }

    public TaskProcessor getTaskProcessor(ObjectProducer[] producers,
                                          ObjectConsumer[] consumers,
                                          ContainerContext containerContext,
                                          ProcessorContext processorContext,
                                          ContainerProcessor processor,
                                          Vertex vertex,
                                          int taskID) {
        checkNotNull(vertex);
        checkNotNull(producers);
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(containerContext);

        if (producers.length == 0) {
            if (consumers.length == 0) {
                return simpleTaskProcessor(processor, containerContext, processorContext, vertex, taskID);
            } else {
                return consumerTaskProcessor(consumers, processor, containerContext, processorContext, vertex, taskID);
            }
        } else {
            if (consumers.length == 0) {
                return producerTaskProcessor(producers, processor, containerContext, processorContext, vertex, taskID);
            } else {
                return actorTaskProcessor(producers, consumers, processor, containerContext, processorContext, vertex, taskID);
            }
        }
    }
}
