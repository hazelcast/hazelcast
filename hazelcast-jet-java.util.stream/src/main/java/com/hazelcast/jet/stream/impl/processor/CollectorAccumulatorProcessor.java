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

import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.processor.TaskContext;
import com.hazelcast.jet.io.Pair;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CollectorAccumulatorProcessor<IN, OUT> extends AbstractStreamProcessor<IN, OUT> {

    private final BiConsumer<OUT, IN> accumulator;
    private final Supplier<OUT> supplier;
    private OUT result;


    public CollectorAccumulatorProcessor(Function<Pair, IN> inputMapper,
                                         Function<OUT, Pair> outputMapper,
                                         BiConsumer<OUT, IN> accumulator,
                                         Supplier<OUT> supplier) {
        super(inputMapper, outputMapper);
        this.accumulator = accumulator;
        this.supplier = supplier;
    }

    @Override
    public void before(TaskContext taskContext) {
        result = supplier.get();
    }

    @Override
    protected boolean process(InputChunk<IN> inputChunk, OutputCollector<OUT> output)
            throws Exception {
        for (IN input : inputChunk) {
            accumulator.accept(result, input);
        }
        return true;
    }

    @Override
    protected boolean finalize(OutputCollector<OUT> output, int chunkSize) throws Exception {
        output.collect(result);
        return true;
    }
}
