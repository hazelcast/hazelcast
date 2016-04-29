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


import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ConsumerTaskProcessor implements TaskProcessor {
    protected static final Object[] DUMMY_CHUNK = new Object[0];
    protected final ObjectConsumer[] consumers;
    protected final ContainerProcessor processor;
    protected final ContainerContext containerContext;
    protected final DefaultObjectIOStream tupleInputStream;
    protected final DefaultObjectIOStream tupleOutputStream;
    protected boolean producersWriteFinished;
    protected final ProcessorContext processorContext;
    protected final ConsumersProcessor consumersProcessor;
    protected boolean consumed;
    protected boolean finalized;
    protected boolean finalizationFinished;
    protected boolean finalizationStarted;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public ConsumerTaskProcessor(ObjectConsumer[] consumers,
                                 ContainerProcessor processor,
                                 ContainerContext containerContext,
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
        this.tupleInputStream = new DefaultObjectIOStream<Object>(DUMMY_CHUNK);
        JetApplicationConfig jetApplicationConfig = containerContext.getConfig();
        int tupleChunkSize = jetApplicationConfig.getChunkSize();
        this.tupleOutputStream = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        reset();
    }

    private void checkFinalization() {
        if ((this.finalizationStarted) && (this.finalizationFinished)) {
            this.finalized = true;
            this.finalizationStarted = false;
            this.finalizationFinished = false;
            resetConsumers();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process() throws Exception {
        if (this.tupleOutputStream.size() > 0) {
            boolean success = onChunk(this.tupleOutputStream);

            if (success) {
                this.tupleOutputStream.reset();
                checkFinalization();
            }

            return success;
        } else {
            if (!this.finalizationStarted) {
                if (this.producersWriteFinished) {
                    return true;
                }

                this.processor.process(
                        this.tupleInputStream,
                        this.tupleOutputStream,
                        null,
                        this.processorContext
                );
            } else {
                this.finalizationFinished = this.processor.finalizeProcessor(this.tupleOutputStream, this.processorContext);
            }

            if (this.tupleOutputStream.size() > 0) {
                boolean success = onChunk(this.tupleOutputStream);

                if (success) {
                    this.tupleOutputStream.reset();
                    checkFinalization();
                }

                return success;
            } else {
                checkFinalization();
            }

            return true;
        }
    }

    @Override
    public boolean onChunk(ProducerInputStream<Object> inputStream) throws Exception {
        this.consumed = false;

        if (inputStream.size() > 0) {
            boolean success = this.consumersProcessor.process(inputStream);
            boolean consumed = this.consumersProcessor.isConsumed();

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
        return this.consumed;
    }

    @Override
    public boolean isFinalized() {
        return this.finalized;
    }

    @Override
    public void reset() {
        resetConsumers();

        this.finalizationStarted = false;
        this.finalizationFinished = false;
        this.producersWriteFinished = false;
        this.finalized = false;
    }

    private void resetConsumers() {
        this.consumed = false;
        this.tupleOutputStream.reset();
        this.consumersProcessor.reset();
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
        this.finalizationStarted = true;
    }

    @Override
    public void onProducersWriteFinished() {
        this.producersWriteFinished = true;
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
