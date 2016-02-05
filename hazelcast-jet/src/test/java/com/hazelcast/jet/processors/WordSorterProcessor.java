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

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.impl.data.tuple.Tuple2;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessorFactory;


public class WordSorterProcessor implements TupleContainerProcessor<String, Integer, String, Integer> {
    private Map<String, Integer> sortedMap;
    private Iterator<Map.Entry<String, Integer>> iterator;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        if (processorContext.getVertex().getDescriptor().getTaskCount() > 1) {
            throw new IllegalStateException();
        }

        sortedMap = new TreeMap<String, Integer>();
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<String, Integer>> inputStream,
                           ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple<String, Integer> tuple : inputStream) {
            sortedMap.put(tuple.getKey(0), tuple.getValue(0));
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        if (iterator == null) {
            iterator = sortedMap.entrySet().iterator();
        }

        int idx = 0;
        int chunkSize = processorContext.getConfig().getChunkSize();

        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            outputStream.consume(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));

            if (idx == chunkSize) {
                return false;
            }

            idx++;
        }

        iterator = null;
        sortedMap = null;
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        @Override
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new WordSorterProcessor();
        }
    }
}
