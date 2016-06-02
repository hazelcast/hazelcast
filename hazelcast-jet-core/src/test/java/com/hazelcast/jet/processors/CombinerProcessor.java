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
import com.hazelcast.jet.api.data.tuple.JetTuple2;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessorFactory;

public class CombinerProcessor implements TupleContainerProcessor<Long, Integer, Long, Integer> {

    Integer result = null;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = null;
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Long, Integer>> inputStream,
                           ConsumerOutputStream<Tuple<Long, Integer>> outputStream,
                           String sourceName, ProcessorContext processorContext) throws Exception {
        for (Tuple<Long, Integer> tuple : inputStream) {
            if (result == null) {
                result = tuple.getValue(0);
            } else {
                result += tuple.getValue(0);
            }

            System.out.println("YatutBil result=" + result + " " + tuple.getValue(0));
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Long, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        if (result != null) {
            outputStream.consume(new JetTuple2<>(0L, result));
        }
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new CombinerProcessor();
        }
    }
}
