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

package com.hazelcast.jet.impl.container.task.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.container.ContainerContext;
import com.hazelcast.jet.impl.container.task.TaskProcessor;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.processor.ContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class SimpleTaskProcessor implements TaskProcessor {
    private static final Object[] DUMMY_CHUNK = new Object[0];

    protected boolean finalizationStarted;
    protected boolean producersWriteFinished;
    private final ContainerProcessor processor;
    private final DefaultObjectIOStream tupleInputStream;
    private final DefaultObjectIOStream tupleOutputStream;
    private boolean finalized;
    private final ProcessorContext processorContext;

    public SimpleTaskProcessor(ContainerProcessor processor,
                               ContainerContext containerContext,
                               ProcessorContext processorContext) {
        checkNotNull(processor);
        this.processor = processor;
        this.processorContext = processorContext;
        this.tupleInputStream = new DefaultObjectIOStream<>(DUMMY_CHUNK);
        this.tupleOutputStream = new DefaultObjectIOStream<>(DUMMY_CHUNK);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process() throws Exception {
        if (!finalizationStarted) {
            if (producersWriteFinished) {
                return true;
            }
            processor.process(tupleInputStream, tupleOutputStream, null, processorContext);
            return true;
        } else {
            finalized = processor.finalizeProcessor(tupleOutputStream, processorContext);
            return true;
        }
    }

    @Override
    public boolean isFinalized() {
        return finalized;
    }

    @Override
    public void reset() {
        finalized = false;
        tupleInputStream.reset();
        tupleOutputStream.reset();
        finalizationStarted = false;
        producersWriteFinished = false;
    }

    @Override
    public void startFinalization() {
        finalizationStarted = true;
    }

    @Override
    public void onProducersWriteFinished() {
        producersWriteFinished = true;
    }

    @Override
    public void onReceiversClosed() {

    }

    @Override
    public boolean producersReadFinished() {
        return true;
    }

    @Override
    public boolean onChunk(ProducerInputStream tupleOutputStream) throws Exception {
        return true;
    }

    @Override
    public boolean produced() {
        return false;
    }

    @Override
    public boolean consumed() {
        return false;
    }

    @Override
    public void onOpen() {
        reset();
    }

    @Override
    public void onClose() {

    }
}
