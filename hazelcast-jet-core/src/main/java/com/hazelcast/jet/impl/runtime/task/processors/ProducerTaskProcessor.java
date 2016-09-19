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

package com.hazelcast.jet.impl.runtime.task.processors;


import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.Processor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.util.Preconditions.checkNotNull;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class ProducerTaskProcessor implements TaskProcessor {
    protected final Producer[] producers;
    protected final Processor processor;
    protected final TaskContext taskContext;
    protected final IOBuffer inputBuffer;
    protected final IOBuffer outputBuffer;
    protected boolean produced;
    protected boolean finalized;
    protected boolean finalizationStarted;
    protected boolean finalizationFinished;
    protected Producer pendingProducer;
    private int nextProducerIdx;

    private boolean producingReadFinished;

    private boolean producersWriteFinished;

    public ProducerTaskProcessor(Producer[] producers,
                                 Processor processor,
                                 TaskContext taskContext) {
        checkNotNull(processor);

        this.producers = producers;
        this.processor = processor;
        this.taskContext = taskContext;
        JobConfig jobConfig = taskContext.getJobContext().getJobConfig();
        int chunkSize = jobConfig.getChunkSize();
        this.inputBuffer = new IOBuffer<>(new Object[chunkSize]);
        this.outputBuffer = new IOBuffer<>(new Object[chunkSize]);
    }

    public boolean onChunk(InputChunk inputChunk) throws Exception {
        return true;
    }

    protected void checkFinalization() {
        if (finalizationStarted && finalizationFinished) {
            finalized = true;
            finalizationStarted = false;
            finalizationFinished = false;
            resetProducers();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process() throws Exception {
        int producersCount = producers.length;

        if (finalizationStarted) {
            finalizationFinished = processor.complete(outputBuffer);

            return !processOutputStream();
        } else if (this.pendingProducer != null) {
            return processProducer(this.pendingProducer);
        }

        return !scanProducers(producersCount);
    }

    private boolean scanProducers(int producersCount) throws Exception {
        int lastIdx = 0;
        boolean produced = false;

        //We should scan all producers if they were marked as closed
        int startFrom = startFrom();

        for (int idx = startFrom; idx < producersCount; idx++) {
            lastIdx = idx;
            Producer producer = this.producers[idx];

            Object[] inChunk = producer.produce();

            if ((JetUtil.isEmpty(inChunk)) || (producer.lastProducedCount() <= 0)) {
                continue;
            }

            produced = true;

            inputBuffer.collect(inChunk, producer.lastProducedCount());
            if (!processProducer(producer)) {
                this.produced = true;
                nextProducerIdx = (idx + 1) % producersCount;
                return true;
            }
        }

        if ((!produced) && (producersWriteFinished)) {
            producingReadFinished = true;
        }

        if (producersCount > 0) {
            nextProducerIdx = (lastIdx + 1) % producersCount;
            this.produced = produced;
        } else {
            this.produced = false;
        }

        return false;
    }

    private int startFrom() {
        return producersWriteFinished ? 0 : nextProducerIdx;
    }

    private boolean processProducer(Producer producer) throws Exception {
        if (!processor.process(inputBuffer, outputBuffer, producer.getName())) {
            pendingProducer = producer;
        } else {
            pendingProducer = null;
        }

        if (!processOutputStream()) {
            produced = true;
            return false;
        }

        outputBuffer.reset();
        return pendingProducer == null;
    }


    private boolean processOutputStream() throws Exception {
        if (outputBuffer.size() == 0) {
            checkFinalization();
            return true;
        } else {
            if (!onChunk(outputBuffer)) {
                produced = true;
                return false;
            } else {
                checkFinalization();
                outputBuffer.reset();
                return true;
            }
        }
    }

    @Override
    public boolean produced() {
        return produced;
    }


    @Override
    public boolean isFinalized() {
        return finalized;
    }

    @Override
    public void reset() {
        resetProducers();

        finalized = false;
        finalizationStarted = false;
        producersWriteFinished = false;
        producingReadFinished = false;
        pendingProducer = null;
    }

    @Override
    public void onOpen() {
        for (Producer producer : this.producers) {
            producer.open();
        }
        reset();
    }

    @Override
    public void onClose() {
        for (Producer producer : this.producers) {
            producer.close();
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
    public boolean producersReadFinished() {
        return producingReadFinished;
    }

    private void resetProducers() {
        produced = false;
        nextProducerIdx = 0;
        outputBuffer.reset();
        inputBuffer.reset();
    }

    @Override
    public boolean consumed() {
        return false;
    }

    @Override
    public void onReceiversClosed() {

    }
}
