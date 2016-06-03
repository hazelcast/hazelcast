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
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class DistinctProcessor<T> extends AbstractStreamProcessor<T, T> {

    private Iterator<T> iterator;
    private final Map<T, Boolean> map = new HashMap<>();

    public DistinctProcessor(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper) {
        super(inputMapper, outputMapper);

    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream, ConsumerOutputStream<T> outputStream) throws Exception {
        for (T t : inputStream) {
            map.put(t, true);
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<T> outputStream, final int chunkSize) throws Exception {
        if (iterator == null) {
            iterator = map.keySet().iterator();
        }
        int i = 0;
        while (iterator.hasNext()) {
            T key = iterator.next();
            outputStream.consume(key);

            if (chunkSize == ++i) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {
        this.iterator = null;
        this.map.clear();
    }

    public static class Factory<T> extends AbstractStreamProcessor.Factory<T, T> {

        public Factory(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper) {
            super(inputMapper, outputMapper);
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<T, Tuple> outputMapper) {
            return new DistinctProcessor<>(inputMapper, outputMapper);
        }
    }
}
