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

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.processor.ContainerProcessor;

import java.util.function.BiConsumer;
import java.util.function.Function;

public class CollectorCombinerProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final BiConsumer<T, T> combiner;
    private T result;

    public CollectorCombinerProcessor(Function<Tuple, T> inputMapper,
                                      Function<T, Tuple> outputMapper,
                                      BiConsumer<T, T> combiner) {
        super(inputMapper, outputMapper);
        this.combiner = combiner;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = null;
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream, ConsumerOutputStream<T> outputStream)
            throws Exception {
        for (T input : inputStream) {
            if (result != null) {
                combiner.accept(result, input);
            } else {
                result = input;
            }
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<T> outputStream, int chunkSize) throws Exception {
        if (result != null) {
            outputStream.consume(result);
        }
        return true;
    }

    public static class Factory<T> extends AbstractStreamProcessor.Factory<T, T> {

        private final BiConsumer<T, T> combiner;

        public Factory(Function<Tuple, T> inputMapper,
                       Function<T, Tuple> outputMapper,
                       BiConsumer<T, T> combiner,
                       Function ignoredFinisher) {
            super(inputMapper, outputMapper);
            this.combiner = combiner;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<T, Tuple> outputMapper) {
            return new CollectorCombinerProcessor<>(inputMapper, outputMapper, combiner);
        }
    }
}
