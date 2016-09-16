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

import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.impl.runtime.task.TaskProcessorFactory;
import com.hazelcast.jet.impl.runtime.task.processors.ActorTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.ProducerTaskProcessor;
import com.hazelcast.jet.impl.runtime.task.processors.SimpleTaskProcessor;
import com.hazelcast.jet.Processor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTaskProcessorFactory implements TaskProcessorFactory {
    @Override
    public TaskProcessor simpleTaskProcessor(Processor processor,
                                             TaskContext taskContext) {
        return new SimpleTaskProcessor(processor, taskContext);
    }

    @Override
    public TaskProcessor consumerTaskProcessor(Consumer[] consumers,
                                               Processor processor,
                                               TaskContext taskContext) {
        return new ConsumerTaskProcessor(consumers, processor, taskContext);
    }

    @Override
    public TaskProcessor producerTaskProcessor(Producer[] producers,
                                               Processor processor,
                                               TaskContext taskContext) {
        return new ProducerTaskProcessor(producers, processor, taskContext);
    }

    @Override
    public TaskProcessor actorTaskProcessor(Producer[] producers,
                                            Consumer[] consumers,
                                            Processor processor,
                                            TaskContext taskContext) {
        return new ActorTaskProcessor(
                producers,
                processor,
                taskContext,
                consumerTaskProcessor(consumers, processor, taskContext)
        );
    }

    public TaskProcessor getTaskProcessor(Producer[] producers,
                                          Consumer[] consumers,
                                          TaskContext taskContext,
                                          Processor processor) {
        checkNotNull(producers);
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(taskContext);

        if (producers.length == 0) {
            if (consumers.length == 0) {
                return simpleTaskProcessor(processor, taskContext);
            } else {
                return consumerTaskProcessor(consumers, processor, taskContext);
            }
        } else {
            if (consumers.length == 0) {
                return producerTaskProcessor(producers, processor, taskContext);
            } else {
                return actorTaskProcessor(producers, consumers, processor, taskContext);
            }
        }
    }
}
