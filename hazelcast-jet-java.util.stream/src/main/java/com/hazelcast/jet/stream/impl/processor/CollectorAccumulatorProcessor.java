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
import java.util.function.Supplier;

public class CollectorAccumulatorProcessor<IN, OUT> extends AbstractStreamProcessor<IN, OUT> {

    private final BiConsumer<OUT, IN> accumulator;
    private final Supplier<OUT> supplier;
    private OUT result;


    public CollectorAccumulatorProcessor(Function<Tuple, IN> inputMapper,
                                         Function<OUT, Tuple> outputMapper,
                                         BiConsumer<OUT, IN> accumulator,
                                         Supplier<OUT> supplier) {
        super(inputMapper, outputMapper);
        this.accumulator = accumulator;
        this.supplier = supplier;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = supplier.get();
    }

    @Override
    protected boolean process(ProducerInputStream<IN> inputStream, ConsumerOutputStream<OUT> outputStream)
            throws Exception {
        for (IN input : inputStream) {
            accumulator.accept(result, input);
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<OUT> outputStream, int chunkSize) throws Exception {
        outputStream.consume(result);
        return true;
    }

    public static class Factory<IN, OUT> extends AbstractStreamProcessor.Factory<IN, OUT> {

        private final BiConsumer<OUT, IN> accumulator;
        private final Supplier<OUT> supplier;

        public Factory(Function<Tuple, IN> inputMapper,
                       Function<OUT, Tuple> outputMapper,
                       BiConsumer<OUT, IN> accumulator, Supplier<OUT> supplier) {
            super(inputMapper, outputMapper);
            this.accumulator = accumulator;
            this.supplier = supplier;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, IN> inputMapper,
                                                                Function<OUT, Tuple> outputMapper) {
            return new CollectorAccumulatorProcessor<>(inputMapper, outputMapper, accumulator, supplier);
        }
    }
}
