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

import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.io.Pair;

import java.util.Iterator;
import java.util.function.Function;

public class LimitProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final long limit;
    private long index;

    public LimitProcessor(Function<Pair, T> inputMapper, Function<T, Pair> outputMapper, Long limit) {
        super(inputMapper, outputMapper);
        this.limit = limit;
    }

    @Override
    public void before(TaskContext context) {
        super.before(context);
        index = 0;
    }

    @Override
    protected boolean process(InputChunk<T> inputChunk,
                              OutputCollector<T> output) throws Exception {
        if (index >= limit) {
            return true;
        }

        for (Iterator<T> iterator = inputChunk.iterator(); iterator.hasNext() && index < limit; index++) {
            logger.info("process: " + index);
            output.collect(iterator.next());
        }
        return true;
    }
}

