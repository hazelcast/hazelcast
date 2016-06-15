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
import com.hazelcast.jet.io.tuple.Tuple;

import java.util.function.BiFunction;
import java.util.function.Function;

public class AccumulatorProcessor<IN, OUT> extends AbstractStreamProcessor<IN, OUT> {

    private final BiFunction<OUT, IN, OUT> accumulator;
    private final OUT identity;
    private OUT result;


    public AccumulatorProcessor(Function<Tuple, IN> inputMapper,
                                Function<OUT, Tuple> outputMapper,
                                BiFunction<OUT, IN, OUT> accumulator,
                                OUT identity) {
        super(inputMapper, outputMapper);
        this.accumulator = accumulator;
        this.identity = identity;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = identity;
    }

    @Override
    protected boolean process(ProducerInputStream<IN> inputStream, ConsumerOutputStream<OUT> outputStream)
            throws Exception {
        for (IN input : inputStream) {
            result = accumulator.apply(result, input);
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<OUT> outputStream, int chunkSize) throws Exception {
        outputStream.consume(result);
        return true;
    }
}
