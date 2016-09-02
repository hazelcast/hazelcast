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

import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.io.Pair;

import java.util.function.Function;

public class SkipProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final long skip;
    private long index;

    public SkipProcessor(Function<Pair, T> inputMapper, Function<T, Pair> outputMapper, Long skip) {
        super(inputMapper, outputMapper);
        this.skip = skip;
    }

    @Override
    public void before(TaskContext context) {
        index = 0;
    }

    @Override
    protected boolean process(InputChunk<T> inputChunk,
                              OutputCollector<T> output) throws Exception {
        for (T input : inputChunk) {
            if (index >= skip) {
                output.collect(input);
            } else {
                index++;
            }
        }
        return true;
    }
}
