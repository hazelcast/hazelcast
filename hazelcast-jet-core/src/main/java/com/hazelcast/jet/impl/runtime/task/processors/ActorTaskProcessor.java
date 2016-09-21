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

import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.Processor;

public class ActorTaskProcessor extends ProducerTaskProcessor {
    protected boolean consumed;

    private final TaskProcessor consumerProcessor;

    public ActorTaskProcessor(Producer[] producers,
                              Processor processor,
                              TaskContext taskContext,
                              TaskProcessor consumerProcessor) {
        super(producers, processor, taskContext);
        this.consumerProcessor = consumerProcessor;
    }

    @Override
    public boolean onChunk(InputChunk inputChunk) throws Exception {
        boolean success = consumerProcessor.onChunk(inputChunk);
        consumed = consumerProcessor.didWork();
        return success;
    }

    @Override
    public boolean process() throws Exception {
        if (outputBuffer.size() == 0) {
            boolean result = super.process();

            if (!produced) {
                consumed = false;
            }

            return result;
        } else {
            boolean success = onChunk(outputBuffer);

            if (success) {
                checkFinalization();
                outputBuffer.reset();
            }

            return success;
        }
    }

    @Override
    public boolean didWork() {
        return consumed || produced;
    }

    @Override
    public void reset() {
        super.reset();
        consumerProcessor.reset();
    }

    @Override
    public void onOpen() {
        super.onOpen();
        consumerProcessor.onOpen();
        reset();
    }

    @Override
    public void onClose() {
        super.onClose();
        consumerProcessor.onClose();
    }

    @Override
    public void startFinalization() {
        super.startFinalization();
        consumerProcessor.startFinalization();
    }
}
