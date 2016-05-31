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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class SortProcessor<T> extends AbstractStreamProcessor<T, T> {

    private final List<T> list ;
    private final Comparator<T> comparator;
    private Iterator<T> iterator;

    public SortProcessor(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper, Comparator<T> comparator) {
        super(inputMapper, outputMapper);
        this.list = new ArrayList<>();
        this.comparator = comparator;
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream, ConsumerOutputStream<T> outputStream) throws Exception {
        for (T t : inputStream) {
            list.add(t);
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<T> outputStream, final int chunkSize) throws Exception {
        if (iterator == null) {
            Collections.sort(list, comparator);
            iterator = list.iterator();
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
        this.list.clear();
    }

    public static class Factory<T> extends AbstractStreamProcessor.Factory<T, T> {

        private final Comparator<T> comparator;

        public Factory(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper, Comparator<T> comparator) {
            super(inputMapper, outputMapper);
            this.comparator = comparator;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<T, Tuple> outputMapper) {
            return new SortProcessor<>(inputMapper, outputMapper, comparator);
        }
    }
}
