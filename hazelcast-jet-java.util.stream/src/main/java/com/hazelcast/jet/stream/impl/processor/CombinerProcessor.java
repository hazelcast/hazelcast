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
import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.jet.spi.processor.ContainerProcessor;

import java.util.function.BinaryOperator;
import java.util.function.Function;

public class CombinerProcessor<T, R> extends AbstractStreamProcessor<T, R> {

    private final BinaryOperator<T> combiner;
    private final Function<T, R> finisher;
    private T result;

    public CombinerProcessor(Function<Tuple, T> inputMapper,
                             Function<R, Tuple> outputMapper,
                             BinaryOperator<T> combiner,
                             Function<T, R> finisher) {
        super(inputMapper, outputMapper);
        this.combiner = combiner;
        this.finisher = finisher;
    }


    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = null;
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream, ConsumerOutputStream<R> outputStream)
            throws Exception {
        for (T input : inputStream) {
            if (result != null) {
                result = combiner.apply(result, input);
            } else {
                result = input;
            }
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<R> outputStream, int chunkSize) throws Exception {
        if (result != null) {
            outputStream.consume(finisher.apply(result));
        }
        return true;
    }

    public static class Factory<T, R> extends AbstractStreamProcessor.Factory<T, R> {

        private final BinaryOperator<T> combiner;
        private final Function<T, R> finisher;

        public Factory(Function<Tuple, T> inputMapper,
                       Function<R, Tuple> outputMapper,
                       BinaryOperator<T> combiner,
                       Function<T, R> finisher) {
            super(inputMapper, outputMapper);
            this.combiner = combiner;
            this.finisher = finisher;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<R, Tuple> outputMapper) {
            return new CombinerProcessor<>(inputMapper, outputMapper, combiner, finisher);
        }
    }
}
