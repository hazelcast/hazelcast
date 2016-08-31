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

import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.ProcessorContext;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class CollectorCombinerProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final BiConsumer<T, T> combiner;
    private T result;

    public CollectorCombinerProcessor(Function<Pair, T> inputMapper,
                                      Function<T, Pair> outputMapper,
                                      BiConsumer<T, T> combiner,
                                      Function ignored) {
        super(inputMapper, outputMapper);
        this.combiner = combiner;
    }

    @Override
    public void before(ProcessorContext processorContext) {
        result = null;
    }

    @Override
    protected boolean process(InputChunk<T> inputChunk, OutputCollector<T> output)
            throws Exception {
        for (T input : inputChunk) {
            if (result != null) {
                combiner.accept(result, input);
            } else {
                result = input;
            }
        }
        return true;
    }

    @Override
    protected boolean finalize(OutputCollector<T> output, int chunkSize) throws Exception {
        if (result != null) {
            output.collect(result);
        }
        return true;
    }
}
