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

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


public class WordSorterProcessor implements ContainerProcessor<Tuple<String, Integer>, Tuple<String, Integer>> {
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
            outputStream.consume(new JetTuple2<String, Integer>(entry.getKey(), entry.getValue()));

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
}
