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

import java.util.Iterator;
import java.util.function.Function;

public class LimitProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final long limit;
    private long index;

    public LimitProcessor(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper, Long limit) {
        super(inputMapper, outputMapper);
        this.limit = limit;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        super.beforeProcessing(processorContext);
        index = 0;
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream,
                              ConsumerOutputStream<T> outputStream) throws Exception {
        if (index >= limit) {
            return true;
        }

        for (Iterator<T> iterator = inputStream.iterator(); iterator.hasNext() && index < limit; index++) {
            this.logger.info("process: " + index);
            outputStream.consume(iterator.next());
        }
        return true;
    }
}

