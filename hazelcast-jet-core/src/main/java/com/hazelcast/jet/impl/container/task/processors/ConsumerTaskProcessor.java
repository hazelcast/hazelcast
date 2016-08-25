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


import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.ObjectConsumer;
import com.hazelcast.jet.impl.container.ContainerContextImpl;
import com.hazelcast.jet.impl.container.task.TaskProcessor;
import com.hazelcast.jet.impl.data.io.ObjectIOStream;
import com.hazelcast.jet.processor.Processor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ConsumerTaskProcessor implements TaskProcessor {
    protected static final Object[] DUMMY_CHUNK = new Object[0];
    protected final ObjectConsumer[] consumers;
    protected final Processor processor;
    protected final ContainerContextImpl containerContext;
    protected final ObjectIOStream pairInputStream;
    protected final ObjectIOStream pairOutputStream;
    protected boolean producersWriteFinished;
    protected final ProcessorContext processorContext;
    protected final ConsumersProcessor consumersProcessor;
    protected boolean consumed;
    protected boolean finalized;
    protected boolean finalizationFinished;
    protected boolean finalizationStarted;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public ConsumerTaskProcessor(ObjectConsumer[] consumers,
                                 Processor processor,
                                 ContainerContextImpl containerContext,
                                 ProcessorContext processorContext) {
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(containerContext);
        checkNotNull(processorContext);

        this.consumers = consumers;
        this.processor = processor;
        this.processorContext = processorContext;
        this.containerContext = containerContext;
        this.consumersProcessor = new ConsumersProcessor(consumers);
        this.pairInputStream = new ObjectIOStream<Object>(DUMMY_CHUNK);
        JobConfig jobConfig = containerContext.getConfig();
        int pairChunkSize = jobConfig.getChunkSize();
        this.pairOutputStream = new ObjectIOStream<Object>(new Object[pairChunkSize]);
        reset();
    }

    private void checkFinalization() {
        if (finalizationStarted && finalizationFinished) {
            finalized = true;
            finalizationStarted = false;
            finalizationFinished = false;
            resetConsumers();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process() throws Exception {
        if (pairOutputStream.size() > 0) {
            return consumeChunkAndResetOutputIfSuccess();
        } else {
            if (finalizationStarted) {
                finalizationFinished = processor.complete(pairOutputStream, processorContext);
            } else {
                if (producersWriteFinished) {
                    return true;
                }
                processor.process(pairInputStream, pairOutputStream, null, processorContext);
            }

            if (pairOutputStream.size() > 0) {
                return consumeChunkAndResetOutputIfSuccess();
            } else {
                checkFinalization();
            }

            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean consumeChunkAndResetOutputIfSuccess() throws Exception {
        boolean success = onChunk(pairOutputStream);

        if (success) {
            pairOutputStream.reset();
            checkFinalization();
        }

        return success;
    }

    @Override
    public boolean onChunk(ProducerInputStream<Object> inputStream) throws Exception {
        this.consumed = false;

        if (inputStream.size() > 0) {
            boolean success = consumersProcessor.process(inputStream);
            boolean consumed = consumersProcessor.isConsumed();

            if (success) {
                resetConsumers();
            }

            this.consumed = consumed;
            return success;
        } else {
            return true;
        }
    }

    @Override
    public boolean consumed() {
        return consumed;
    }

    @Override
    public boolean isFinalized() {
        return finalized;
    }

    @Override
    public void reset() {
        resetConsumers();
        finalizationStarted = false;
        finalizationFinished = false;
        producersWriteFinished = false;
        finalized = false;
    }

    private void resetConsumers() {
        consumed = false;
        pairOutputStream.reset();
        consumersProcessor.reset();
    }

    public void onOpen() {
        for (ObjectConsumer consumer : consumers) {
            consumer.open();
        }
        reset();
    }

    @Override
    public void onClose() {
        reset();
        for (ObjectConsumer consumer : consumers) {
            if (!consumer.isShuffled()) {
                consumer.close();
            }
        }
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
    public boolean produced() {
        return false;
    }
}
