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
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessorFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ListProcessor implements TupleContainerProcessor<Integer, String, Integer, String> {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Map<Integer, Tuple<Integer, String>> list = new ConcurrentSkipListMap<Integer, Tuple<Integer, String>>();

    private static volatile String activeNode;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        activeNode = null;
        counter.set(0);
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Integer, String>> inputStream,
                           ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        activeNode = processorContext.getNodeEngine().getHazelcastInstance().getName();

        for (Tuple<Integer, String> tuple : inputStream) {
            list.put(tuple.getKey(0), tuple);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream, ProcessorContext processorContext) throws Exception {
        if ((activeNode != null) && (activeNode.equals(processorContext.getNodeEngine().getHazelcastInstance().getName()))) {
            if (counter.incrementAndGet() >= processorContext.getVertex().getDescriptor().getTaskCount()) {
                System.out.println("Eventually I am writing");

                for (Map.Entry<Integer, Tuple<Integer, String>> integerTupleEntry : list.entrySet()) {
                    outputStream.consume(integerTupleEntry.getValue());
                }

                System.out.println("Eventually I've written");
            }
        }

        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new ListProcessor();
        }
    }
}
