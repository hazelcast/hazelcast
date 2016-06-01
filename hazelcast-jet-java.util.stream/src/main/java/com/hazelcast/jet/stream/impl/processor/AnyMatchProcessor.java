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

import java.util.function.Function;
import java.util.function.Predicate;


public class AnyMatchProcessor<T> extends AbstractStreamProcessor<T, Boolean> {

    private boolean match;
    private final Predicate<T> predicate;

    public AnyMatchProcessor(Function<Tuple, T> inputMapper, Function<Boolean, Tuple> outputMapper,
                             Predicate<T> predicate) {
        super(inputMapper, outputMapper);

        this.predicate = predicate;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        super.beforeProcessing(processorContext);
        match = false;
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream,
                              ConsumerOutputStream<Boolean> outputStream) throws Exception {
        if (match) {
            return true;
        }

        for (T t : inputStream) {
            if (predicate.test(t)) {
                match = true;
                return true;
            }
        }
        return true;
    }

    @Override
    protected boolean finalize(ConsumerOutputStream<Boolean> outputStream, final int chunkSize) throws Exception {
        outputStream.consume(match);
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {
    }

    public static class Factory<T> extends AbstractStreamProcessor.Factory<T, Boolean> {

        private final Predicate<T> predicate;

        public Factory(Function<Tuple, T> inputMapper, Function<Boolean, Tuple> outputMapper, Predicate<T> predicate) {
            super(inputMapper, outputMapper);
            this.predicate = predicate;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<Boolean, Tuple> outputMapper) {
            return new AnyMatchProcessor<>(inputMapper, outputMapper, predicate);
        }
    }

}
