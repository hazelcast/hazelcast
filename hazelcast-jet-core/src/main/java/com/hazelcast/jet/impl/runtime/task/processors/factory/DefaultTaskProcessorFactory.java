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

package com.hazelcast.jet.impl.runtime.task.processors.factory;

import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.actor.Consumer;
import com.hazelcast.jet.impl.actor.Producer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.impl.runtime.task.TaskProcessorFactory;
import com.hazelcast.jet.impl.runtime.task.processors.ActorTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.ProducerTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.SimpleTaskProcessor;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTaskProcessorFactory implements TaskProcessorFactory {
    @Override
    public TaskProcessor simpleTaskProcessor(Processor processor,
                                             JobContext jobContext,
                                             ProcessorContext processorContext,
                                             Vertex vertex,
                                             int taskID) {
        return new SimpleTaskProcessor(processor, processorContext);
    }

    @Override
    public TaskProcessor consumerTaskProcessor(Consumer[] consumers,
                                               Processor processor,
                                               JobContext jobContext,
                                               ProcessorContext processorContext,
                                               Vertex vertex,
                                               int taskID) {
        return new ConsumerTaskProcessor(consumers, processor, jobContext, processorContext);
    }

    @Override
    public TaskProcessor producerTaskProcessor(Producer[] producers,
                                               Processor processor,
                                               JobContext jobContext,
                                               ProcessorContext processorContext,
                                               Vertex vertex,
                                               int taskID) {
        return new ProducerTaskProcessor(producers, processor, jobContext, processorContext, taskID);
    }

    @Override
    public TaskProcessor actorTaskProcessor(Producer[] producers,
                                            Consumer[] consumers,
                                            Processor processor,
                                            JobContext jobContext,
                                            ProcessorContext processorContext,
                                            Vertex vertex,
                                            int taskID) {
        return new ActorTaskProcessor(
                producers,
                processor,
                jobContext,
                processorContext,
                consumerTaskProcessor(consumers, processor, jobContext, processorContext, vertex, taskID),
                taskID
        );
    }

    public TaskProcessor getTaskProcessor(Producer[] producers,
                                          Consumer[] consumers,
                                          JobContext jobContext,
                                          ProcessorContext processorContext,
                                          Processor processor,
                                          Vertex vertex,
                                          int taskID) {
        checkNotNull(vertex);
        checkNotNull(producers);
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(jobContext);

        if (producers.length == 0) {
            if (consumers.length == 0) {
                return simpleTaskProcessor(processor, jobContext, processorContext, vertex, taskID);
            } else {
                return consumerTaskProcessor(consumers, processor, jobContext, processorContext, vertex, taskID);
            }
        } else {
            if (consumers.length == 0) {
                return producerTaskProcessor(producers, processor, jobContext, processorContext, vertex, taskID);
            } else {
                return actorTaskProcessor(producers, consumers, processor, jobContext, processorContext, vertex, taskID);
            }
        }
    }
}
