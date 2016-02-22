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


import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.processor.ContainerProcessor;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ProducerTaskProcessor implements TaskProcessor {
    protected final int taskID;
    protected final ObjectProducer[] producers;
    protected final ContainerProcessor processor;
    protected final ContainerContext containerContext;
    protected final ProcessorContext processorContext;
    protected final DefaultObjectIOStream objectInputStream;
    protected final DefaultObjectIOStream tupleOutputStream;
    protected boolean produced;
    protected boolean finalized;
    protected boolean finalizationStarted;
    protected boolean finalizationFinished;
    protected ObjectProducer pendingProducer;
    private int nextProducerIdx;

    private boolean producingReadFinished;

    private boolean producersWriteFinished;

    public ProducerTaskProcessor(ObjectProducer[] producers,
                                 ContainerProcessor processor,
                                 ContainerContext containerContext,
                                 ProcessorContext processorContext,
                                 int taskID) {
        checkNotNull(processor);

        this.taskID = taskID;
        this.producers = producers;
        this.processor = processor;
        this.processorContext = processorContext;
        this.containerContext = containerContext;
        JetApplicationConfig jetApplicationConfig = containerContext.getApplicationContext().getJetApplicationConfig();
        int tupleChunkSize = jetApplicationConfig.getChunkSize();
        this.objectInputStream = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        this.tupleOutputStream = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
    }

    public boolean onChunk(ProducerInputStream inputStream) throws Exception {
        return true;
    }

    protected void checkFinalization() {
        if ((this.finalizationStarted) && (this.finalizationFinished)) {
            this.finalized = true;
            this.finalizationStarted = false;
            this.finalizationFinished = false;
            resetProducers();
        }
    }

    @Override
    public boolean process() throws Exception {
        int producersCount = this.producers.length;

        boolean produced = false;

        if (this.finalizationStarted) {
            this.finalizationFinished = this.processor.finalizeProcessor(
                    this.tupleOutputStream,
                    this.processorContext
            );

            return !processOutputStream();
        } else if (this.pendingProducer != null) {
            return processProducer(this.pendingProducer);
        }

        int lastIdx = 0;

        //We should scan all producers if they were marked as closed
        int startFrom = startFrom();

        for (int i = startFrom; i < producersCount; i++) {
            lastIdx = i;
            ObjectProducer producer = this.producers[i];

            Object[] inChunk = producer.produce();

            if ((JetUtil.isEmpty(inChunk)) || (producer.lastProducedCount() <= 0)) {
                continue;
            }

            produced = true;

            this.objectInputStream.consumeChunk(
                    inChunk,
                    producer.lastProducedCount()
            );

            if (!processProducer(producer)) {
                this.nextProducerIdx = (i + 1) % producersCount;
                return false;
            }
        }

        if ((!produced) && (this.producersWriteFinished)) {
            this.producingReadFinished = true;
        }

        if (producersCount > 0) {
            this.nextProducerIdx = (lastIdx + 1) % producersCount;
            this.produced = produced;
        }

        return true;
    }

    private int startFrom() {
        return this.producersWriteFinished ? 0 : this.nextProducerIdx;
    }

    private boolean processProducer(ObjectProducer producer) throws Exception {
        if (!this.processor.process(
                this.objectInputStream,
                this.tupleOutputStream,
                producer.getName(),
                this.processorContext
        )) {
            this.pendingProducer = producer;
        } else {
            this.pendingProducer = null;
        }

        if (!processOutputStream()) {
            this.produced = true;
            return false;
        }

        this.tupleOutputStream.reset();
        return this.pendingProducer == null;
    }


    private boolean processOutputStream() throws Exception {
        if (this.tupleOutputStream.size() == 0) {
            checkFinalization();
            return true;
        } else {
            if (!onChunk(this.tupleOutputStream)) {
                this.produced = true;
                return false;
            } else {
                checkFinalization();
                this.tupleOutputStream.reset();
                return true;
            }
        }
    }

    @Override
    public boolean produced() {
        return this.produced;
    }


    @Override
    public boolean isFinalized() {
        return this.finalized;
    }

    @Override
    public boolean hasActiveProducers() {
        return this.producers.length > 0;
    }

    @Override
    public void reset() {
        resetProducers();

        this.finalized = false;
        this.finalizationStarted = false;
        this.producersWriteFinished = false;
        this.producingReadFinished = false;
        this.pendingProducer = null;
    }

    @Override
    public void onOpen() {
        for (ObjectProducer producer : this.producers) {
            producer.open();
        }

        reset();
    }

    @Override
    public void onClose() {
        for (ObjectProducer producer : this.producers) {
            producer.close();
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
    public boolean producersReadFinished() {
        return this.producingReadFinished;
    }

    private void resetProducers() {
        this.produced = false;
        this.nextProducerIdx = 0;
        this.tupleOutputStream.reset();
        this.objectInputStream.reset();
    }

    @Override
    public boolean consumed() {
        return false;
    }

    @Override
    public boolean hasActiveConsumers() {
        return false;
    }

    @Override
    public void onReceiversClosed() {

    }
}
