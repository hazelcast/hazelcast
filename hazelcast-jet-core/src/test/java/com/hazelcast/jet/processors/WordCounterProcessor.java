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
import com.hazelcast.jet.processor.ContainerProcessorFactory;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.JetTuple;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCounterProcessor implements ContainerProcessor<String, JetTuple<String, Integer>> {


    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private Iterator<Object> finalizationIterator;

    private Map<Object, Integer> cache = new HashMap<Object, Integer>();

    public WordCounterProcessor() {

    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        taskCounter.set(processorContext.getVertex().getDescriptor().getTaskCount());
    }

    @Override
    public boolean process(ProducerInputStream<String> inputStream,
                           ConsumerOutputStream<JetTuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (String word : inputStream) {
            Integer value = this.cache.get(word);

            if (value == null) {
                this.cache.put(word, 1);
            } else {
                this.cache.put(word, value + 1);
            }
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<JetTuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;

        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.keySet().iterator();
            }

            int idx = 0;
            int chunkSize = processorContext.getConfig().getChunkSize();

            while (this.finalizationIterator.hasNext()) {
                String word = (String) this.finalizationIterator.next();

                outputStream.consume(new JetTuple2<String, Integer>(word, this.cache.get(word)));

                if (idx == chunkSize - 1) {
                    break;
                }

                idx++;
            }

            finalized = !this.finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                this.finalizationIterator = null;
                clearCaches();
            }
        }

        return finalized;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    private void clearCaches() {
        this.cache.clear();
    }

    public static class Factory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new WordCounterProcessor();
        }
    }
}
