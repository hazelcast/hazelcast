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

import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.impl.actor.Producer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;

public class ActorTaskProcessor extends ProducerTaskProcessor {
    protected boolean consumed;

    private final TaskProcessor consumerProcessor;

    public ActorTaskProcessor(Producer[] producers,
                              Processor processor,
                              JobContext jobContext,
                              ProcessorContext processorContext,
                              TaskProcessor consumerProcessor,
                              int taskID) {
        super(producers, processor, jobContext, processorContext, taskID);
        this.consumerProcessor = consumerProcessor;
    }

    public boolean onChunk(InputChunk inputChunk) throws Exception {
        boolean success = this.consumerProcessor.onChunk(inputChunk);
        this.consumed = this.consumerProcessor.consumed();
        return success;
    }

    public boolean process() throws Exception {
        if (this.outputBuffer.size() == 0) {
            boolean result = super.process();

            if (!this.produced) {
                this.consumed = false;
            }

            return result;
        } else {
            boolean success = onChunk(this.outputBuffer);

            if (success) {
                checkFinalization();
                this.outputBuffer.reset();
            }

            return success;
        }
    }

    @Override
    public boolean consumed() {
        return this.consumed;
    }

    @Override
    public void reset() {
        super.reset();
        this.consumerProcessor.reset();
    }

    @Override
    public void onOpen() {
        super.onOpen();
        this.consumerProcessor.onOpen();
        reset();
    }

    @Override
    public void onClose() {
        super.onClose();
        this.consumerProcessor.onClose();
    }

    public void startFinalization() {
        super.startFinalization();
        this.consumerProcessor.startFinalization();
    }
}
