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

package com.hazelcast.jet.processors;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.tuple.JetTuple;
import com.hazelcast.jet.api.processor.ContainerProcessor;

import java.util.Iterator;
import java.util.StringTokenizer;

public class WordGeneratorProcessor implements ContainerProcessor<Object, String> {
    private int idx;
    private Iterator<Object> iterator;
    private StringTokenizer stringTokenizer;

    private boolean processStringTokenizer(ConsumerOutputStream<String> outputStream,
                                           ProcessorContext processorContext) throws Exception {
        while (this.stringTokenizer.hasMoreElements()) {
            String word = this.stringTokenizer.nextToken();

            outputStream.consume(word);

            this.idx++;

            if (this.idx == processorContext.getConfig().getChunkSize()) {
                this.idx = 0;
                return false;
            }
        }

        this.idx = 0;
        this.stringTokenizer = null;
        return true;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        this.idx = 0;
        this.iterator = null;
        this.stringTokenizer = null;
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<String> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        if (this.stringTokenizer != null) {
            processStringTokenizer(outputStream, processorContext);
            return false;
        }

        if (this.iterator == null) {
            this.iterator = inputStream.iterator();
        }

        while (iterator.hasNext()) {
            Object object = iterator.next();
            String text;

            if (object instanceof String) {
                text = (String) object;
            } else if (object instanceof JetTuple) {
                text = (String) ((JetTuple) object).getValue(0);
            } else {
                if (object == null) {
                    throw new IllegalStateException("Null object");
                } else {
                    throw new IllegalStateException("Wrong object: " + object);
                }
            }

            this.stringTokenizer = new StringTokenizer(text);

            if (!processStringTokenizer(outputStream, processorContext)) {
                return false;
            }
        }

        this.iterator = null;
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<String> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        if (this.stringTokenizer != null) {
            processStringTokenizer(outputStream, processorContext);
            return false;
        }

        this.stringTokenizer = null;
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements ContainerProcessorFactory<Object, String> {
        @Override
        public ContainerProcessor<Object, String> getProcessor(Vertex vertex) {
            return new WordGeneratorProcessor();
        }
    }
}
