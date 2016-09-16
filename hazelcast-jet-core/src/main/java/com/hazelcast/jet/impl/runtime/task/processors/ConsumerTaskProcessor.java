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


import com.hazelcast.jet.Processor;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.TaskContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ConsumerTaskProcessor implements TaskProcessor {
    protected static final Object[] DUMMY_CHUNK = new Object[0];
    protected final Consumer[] consumers;
    protected final Processor processor;
    protected final IOBuffer inputBuffer;
    protected final IOBuffer outputBuffer;
    protected boolean producersWriteFinished;
    protected final TaskContext taskContext;
    protected final ConsumersProcessor consumersProcessor;
    protected boolean consumed;
    protected boolean finalized;
    protected boolean finalizationFinished;
    protected boolean finalizationStarted;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public ConsumerTaskProcessor(Consumer[] consumers,
                                 Processor processor,
                                 TaskContext taskContext) {
        checkNotNull(consumers);
        checkNotNull(processor);
        checkNotNull(taskContext);

        this.consumers = consumers;
        this.processor = processor;
        this.taskContext = taskContext;
        this.consumersProcessor = new ConsumersProcessor(consumers);
        this.inputBuffer = new IOBuffer<Object>(DUMMY_CHUNK);
        JobConfig jobConfig = taskContext.getJobContext().getJobConfig();
        int chunkSize = jobConfig.getChunkSize();
        this.outputBuffer = new IOBuffer<Object>(new Object[chunkSize]);
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
        if (outputBuffer.size() > 0) {
            return consumeChunkAndResetOutputIfSuccess();
        } else {
            if (finalizationStarted) {
                finalizationFinished = processor.complete(outputBuffer);
            } else {
                if (producersWriteFinished) {
                    return true;
                }
                processor.process(inputBuffer, outputBuffer, null);
            }

            if (outputBuffer.size() > 0) {
                return consumeChunkAndResetOutputIfSuccess();
            } else {
                checkFinalization();
            }

            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean consumeChunkAndResetOutputIfSuccess() throws Exception {
        boolean success = onChunk(outputBuffer);

        if (success) {
            outputBuffer.reset();
            checkFinalization();
        }

        return success;
    }

    @Override
    public boolean onChunk(InputChunk<Object> inputChunk) throws Exception {
        this.consumed = false;

        if (inputChunk.size() > 0) {
            boolean success = consumersProcessor.process(inputChunk);
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
        outputBuffer.reset();
        consumersProcessor.reset();
    }

    public void onOpen() {
        for (Consumer consumer : consumers) {
            consumer.open();
        }
        reset();
    }

    @Override
    public void onClose() {
        reset();
        for (Consumer consumer : consumers) {
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
