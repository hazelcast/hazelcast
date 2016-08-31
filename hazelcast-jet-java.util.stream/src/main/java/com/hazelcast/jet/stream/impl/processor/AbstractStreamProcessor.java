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
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.logging.ILogger;

import java.util.Iterator;
import java.util.function.Function;

abstract class AbstractStreamProcessor<IN, OUT> implements Processor<Pair, Pair> {

    protected ILogger logger;

    private final MappingInputChunk mappingInput;
    private final MappingOutputCollector mappingOutput;

    public AbstractStreamProcessor(Function<Pair, IN> inputMapper,
                                   Function<OUT, Pair> outputMapper) {

        this.mappingInput = new MappingInputChunk(inputMapper);
        this.mappingOutput = new MappingOutputCollector(outputMapper);
    }

    @Override
    public void before(ProcessorContext processorContext) {
        if (logger == null) {
            this.logger = processorContext.getNodeEngine().getLogger(this.getClass().getName()
                    + "." + processorContext.getVertex().getName());
        }
    }

    @Override
    public boolean process(InputChunk<Pair> input, OutputCollector<Pair> output,
                           String sourceName, ProcessorContext context) throws Exception {
        mappingInput.setInput(input);
        mappingOutput.setOutput(output);
        return process(mappingInput, mappingOutput);
    }

    @Override
    public boolean complete(OutputCollector<Pair> output,
                            ProcessorContext processorContext) throws Exception {
        mappingOutput.setOutput(output);
        return finalize(mappingOutput, processorContext.getConfig().getChunkSize());
    }

    protected abstract boolean process(InputChunk<IN> input,
                                       OutputCollector<OUT> output) throws Exception;

    protected boolean finalize(OutputCollector<OUT> outputCollectorStream, final int chunkSize) throws Exception {
        return true;
    }

    public class MappingInputChunk implements InputChunk<IN> {

        private InputChunk<Pair> input;
        private final Function<Pair, IN> inputMapper;

        public MappingInputChunk(Function<Pair, IN> inputMapper) {
            this.inputMapper = inputMapper;
        }

        private void setInput(InputChunk<Pair> input) {
            this.input = input;
        }

        @Override
        public IN get(int idx) {
            return inputMapper.apply(input.get(idx));
        }

        @Override
        public int size() {
            return input.size();
        }

        @Override
        public Iterator<IN> iterator() {
            Iterator<Pair> iterator = input.iterator();

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

    public class MappingOutputCollector implements OutputCollector<OUT> {

        private OutputCollector<Pair> outputCollector;
        private final Function<OUT, Pair> outputMapper;

        public MappingOutputCollector(Function<OUT, Pair> outputMapper) {
            this.outputMapper = outputMapper;
        }

        private void setOutput(OutputCollector<Pair> outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void collect(InputChunk<OUT> chunk) {
            for (OUT out : chunk) {
                outputCollector.collect(outputMapper.apply(out));
            }
        }

        @Override
        public void collect(OUT[] chunk, int size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void collect(OUT object) {
            outputCollector.collect(outputMapper.apply(object));
        }
    }
}
