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
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collector;

public class GroupingAccumulatorProcessor<K, V, A, R> implements ContainerProcessor<Tuple<K, V>, Tuple<K, A>> {

    private final Map<K, A> cache = new HashMap<>();
    private final Collector<V, A, R> collector;
    private Iterator<Map.Entry<K, A>> finalizationIterator;

    public GroupingAccumulatorProcessor(Collector<V, A, R> collector) {

        this.collector = collector;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream<Tuple<K, V>> inputStream,
                           ConsumerOutputStream<Tuple<K, A>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple<K, V> input : inputStream) {
            A value = this.cache.get(input.getKey(0));
            if (value == null) {
                value = collector.supplier().get();
                this.cache.put(input.getKey(0), value);
            }
            collector.accumulator().accept(value, input.getValue(0));
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<K, A>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;
        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.entrySet().iterator();
            }

            int idx = 0;
            while (this.finalizationIterator.hasNext()) {
                Map.Entry<K, A> next = this.finalizationIterator.next();
                outputStream.consume(new JetTuple2<>(next.getKey(), next.getValue()));

                if (idx == processorContext.getConfig().getChunkSize() - 1) {
                    break;
                }
                idx++;
            }

            finalized = !this.finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                this.finalizationIterator = null;
                this.cache.clear();
            }
        }
        return finalized;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory<K, V, A, R> implements ContainerProcessorFactory<Tuple<K, V>, Tuple<K, A>> {


        private final Collector<V, A, R> collector;

        public Factory(Collector<V, A, R> collector) {
            this.collector = collector;
        }

        @Override
        public ContainerProcessor<Tuple<K, V>, Tuple<K, A>> getProcessor(Vertex vertex) {
            return new GroupingAccumulatorProcessor<>(collector);
        }
    }
}
