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
import com.hazelcast.logging.ILogger;

import java.util.Iterator;
import java.util.function.Function;

abstract class AbstractStreamProcessor<IN, OUT> implements ContainerProcessor<Tuple, Tuple> {

    protected ILogger logger;

    private final MappingProducerInputStream mappinginputStream;
    private final MappingConsumerOutputStream mappingOutputStream;

    public AbstractStreamProcessor(Function<Tuple, IN> inputMapper,
                                   Function<OUT, Tuple> outputMapper) {

        this.mappinginputStream = new MappingProducerInputStream(inputMapper);
        this.mappingOutputStream = new MappingConsumerOutputStream(outputMapper);
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        if (logger == null) {
            this.logger = processorContext.getNodeEngine().getLogger(this.getClass().getName()
                    + "." + processorContext.getVertex().getName());
        }
    }

    @Override
    public boolean process(ProducerInputStream<Tuple> inputStream, ConsumerOutputStream<Tuple> outputStream,
                           String sourceName, ProcessorContext processorContext) throws Exception {
        mappinginputStream.setInputStream(inputStream);
        mappingOutputStream.setOutputStream(outputStream);
        return process(mappinginputStream, mappingOutputStream);
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        mappingOutputStream.setOutputStream(outputStream);
        return finalize(mappingOutputStream, processorContext.getConfig().getChunkSize());
    }

    protected abstract boolean process(ProducerInputStream<IN> inputStream,
                                       ConsumerOutputStream<OUT> outputStream) throws Exception;

    protected boolean finalize(ConsumerOutputStream<OUT> outputStream, final int chunkSize) throws Exception {
        return true;
    }

    public class MappingProducerInputStream implements ProducerInputStream<IN> {

        private ProducerInputStream<Tuple> inputStream;
        private final Function<Tuple, IN> inputMapper;

        public MappingProducerInputStream(Function<Tuple, IN> inputMapper) {
            this.inputMapper = inputMapper;
        }

        private void setInputStream(ProducerInputStream<Tuple> inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public IN get(int idx) {
            return inputMapper.apply(inputStream.get(idx));
        }

        @Override
        public int size() {
            return inputStream.size();
        }

        @Override
        public Iterator<IN> iterator() {
            Iterator<Tuple> iterator = inputStream.iterator();

            return new Iterator<IN>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public IN next() {
                    return inputMapper.apply(iterator.next());
                }
            };
        }
    }

    public class MappingConsumerOutputStream implements ConsumerOutputStream<OUT> {

        private ConsumerOutputStream<Tuple> outputStream;
        private final Function<OUT, Tuple> outputMapper;

        public MappingConsumerOutputStream(Function<OUT, Tuple> outputMapper) {
            this.outputMapper = outputMapper;
        }

        private void setOutputStream(ConsumerOutputStream<Tuple> outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void consumeStream(ProducerInputStream<OUT> inputStream) throws Exception {
            for (OUT out : inputStream) {
                outputStream.consume(outputMapper.apply(out));
            }
        }

        @Override
        public void consumeChunk(OUT[] chunk, int actualSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean consume(OUT object) throws Exception {
            return outputStream.consume(outputMapper.apply(object));
        }
    }
}
