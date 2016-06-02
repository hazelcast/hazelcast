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

import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.processor.ContainerProcessor;

import java.util.function.Function;

public class PassthroughProcessor<T> extends AbstractStreamProcessor<T, T> {

    public PassthroughProcessor(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper) {
        super(inputMapper, outputMapper);
    }

    @Override
    protected boolean process(ProducerInputStream<T> inputStream,
                              ConsumerOutputStream<T> outputStream) throws Exception {
        outputStream.consumeStream(inputStream);
        return true;
    }

    public static class Factory<T> extends AbstractStreamProcessor.Factory<T, T> {

        public Factory(Function<Tuple, T> inputMapper, Function<T, Tuple> outputMapper) {
            super(inputMapper, outputMapper);
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, T> inputMapper,
                                                                Function<T, Tuple> outputMapper) {
            return new PassthroughProcessor<>(inputMapper, outputMapper);
        }
    }
}
