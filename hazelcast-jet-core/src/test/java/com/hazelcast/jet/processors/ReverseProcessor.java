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

package com.hazelcast.jet.processors;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessorFactory;

public class ReverseProcessor implements TupleContainerProcessor<Integer, String, String, Integer> {
    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Integer, String>> inputStream,
                           ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple tuple : inputStream) {
            Object key = tuple.getKey(0);
            Object value = tuple.getValue(0);
            tuple.setKey(0, value);
            tuple.setValue(0, key);
            outputStream.consume(tuple);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new ReverseProcessor();
        }
    }
}
