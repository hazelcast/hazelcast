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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.processor.ContainerProcessorFactory;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.processor.ContainerProcessor;

public class EmptyProcessor implements ContainerProcessor<Tuple, Tuple> {
    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream<Tuple> inputStream, ConsumerOutputStream<Tuple> outputStream,
                           String sourceName, ProcessorContext processorContext) throws Exception {
        outputStream.consumeStream(inputStream);
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements ContainerProcessorFactory<Tuple, Tuple> {
        public ContainerProcessor<Tuple, Tuple> getProcessor(Vertex vertex) {
            return new EmptyProcessor();
        }
    }
}
