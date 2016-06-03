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

import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.stream.impl.pipeline.TransformOperation;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TransformProcessor extends AbstractStreamProcessor<Object, Object> {

    private final TransformOperation[] operations;

    public static class Factory extends AbstractStreamProcessor.Factory<Object, Object> {

        private final List<TransformOperation> operations;

        public Factory(Function<Tuple, Object> inputMapper, Function<Object, Tuple> outputMapper,
                       List<TransformOperation> operations) {
            super(inputMapper, outputMapper);
            this.operations = operations;
        }

        @Override
        protected ContainerProcessor<Tuple, Tuple> getProcessor(Function<Tuple, Object> inputMapper,
                                                                Function<Object, Tuple> outputMapper) {
            return new TransformProcessor(inputMapper, outputMapper, operations);
        }
    }


    public TransformProcessor(Function<Tuple, Object> inputMapper, Function<Object, Tuple> outputMapper,
                              List<TransformOperation> operations) {
        super(inputMapper, outputMapper);
        this.operations = operations.toArray(new TransformOperation[operations.size()]);
    }

    @Override
    protected boolean process(ProducerInputStream<Object> inputStream,
                              ConsumerOutputStream<Object> outputStream) throws Exception {
        for (Object input : inputStream) {
            processInputs(input, outputStream, 0);
        }
        return true;
    }

    private void processInputs(Object input, ConsumerOutputStream<Object> outputStream, int startIndex) throws Exception {
        for (int i = startIndex; i < operations.length; i++) {
            TransformOperation operation = operations[i];
            switch (operation.getType()) {
                case FILTER:
                    if (!((Predicate) operation.getFunction()).test(input)) {
                        return;
                    }
                    break;
                case MAP:
                    input = ((Function) operation.getFunction()).apply(input);
                    break;
                case FLAT_MAP:
                    Stream stream = (Stream) ((Function) operation.getFunction()).apply(input);
                    Iterator iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        processInputs(iterator.next(), outputStream, i + 1);
                    }
                    return;
                default:
                    throw new JetException("Unknown case: " + operation.getType());
            }
        }
        outputStream.consume(input);
    }
}
