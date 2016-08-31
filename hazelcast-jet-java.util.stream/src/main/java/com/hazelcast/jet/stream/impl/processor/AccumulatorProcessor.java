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
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.io.Pair;

import java.util.function.BiFunction;
import java.util.function.Function;

public class AccumulatorProcessor<IN, OUT> extends AbstractStreamProcessor<IN, OUT> {

    private final BiFunction<OUT, IN, OUT> accumulator;
    private final OUT identity;
    private OUT result;


    public AccumulatorProcessor(Function<Pair, IN> inputMapper,
                                Function<OUT, Pair> outputMapper,
                                BiFunction<OUT, IN, OUT> accumulator,
                                OUT identity) {
        super(inputMapper, outputMapper);
        this.accumulator = accumulator;
        this.identity = identity;
    }

    @Override
    public void before(ProcessorContext processorContext) {
        result = identity;
    }

    @Override
    protected boolean process(InputChunk<IN> inputChunk, OutputCollector<OUT> output)
            throws Exception {
        for (IN input : inputChunk) {
            result = accumulator.apply(result, input);
        }
        return true;
    }

    @Override
    protected boolean finalize(OutputCollector<OUT> output, int chunkSize) throws Exception {
        output.collect(result);
        return true;
    }
}
