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
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.ringbuffer.ShufflingReceiver;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.TaskProcessor;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.List;

public class ShuffledActorTaskProcessor extends ActorTaskProcessor {
    private final Producer[] receivers;
    private final IOBuffer<Object> receiveBuffer;
    private final TaskProcessor receiverConsumerProcessor;
    private int nextReceiverIdx;
    private boolean receiversClosed;
    private boolean receiversProduced;

    public ShuffledActorTaskProcessor(
            Producer[] producers, Processor processor, TaskContext taskContext,
            TaskProcessor senderConsumerProcessor, TaskProcessor receiverConsumerProcessor) {
        super(producers, processor, taskContext, senderConsumerProcessor);
        this.receiverConsumerProcessor = receiverConsumerProcessor;
        List<Producer> receivers = new ArrayList<Producer>();
        JobContext jobContext = taskContext.getJobContext();
        JobManager jobManager = jobContext.getJobManager();
        VertexRunner vertexRunner = jobManager.getRunnerByVertex(taskContext.getVertex());
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskContext.getTaskNumber());
        for (Address address : jobManager.getJobContext().getSocketReaders().keySet()) {
            ShufflingReceiver receiver = new ShufflingReceiver(vertexTask);
            jobManager.registerShufflingReceiver(taskContext.getTaskNumber(),
                    vertexRunner.getId(), address, receiver);
            receivers.add(receiver);
        }
        int chunkSize = jobContext.getJobConfig().getChunkSize();
        this.receiveBuffer = new IOBuffer<>(new Object[chunkSize]);
        this.receivers = receivers.toArray(new Producer[receivers.size()]);
    }

    @Override
    public void onOpen() {
        super.onOpen();
        for (Producer receiver : receivers) {
            receiver.open();
        }
    }

    @Override
    public void reset() {
        super.reset();
        receiversClosed = false;
    }

    @Override
    public boolean process() throws Exception {
        if (receiveBuffer.size() > 0) {
            produced = false;
            receiversProduced = false;
            if (receiverConsumerProcessor.onChunk(receiveBuffer)) {
                receiveBuffer.reset();
                return true;
            }
            return false;
        }
        if (outputBuffer.size() > 0) {
            return super.process();
        }
        boolean success;
        if (receivers.length > 0) {
            success = processReceivers();
            if (success) {
                receiveBuffer.reset();
            }
        } else {
            receiversProduced = false;
            success = true;
        }
        success = success && super.process();
        produced = receiversProduced || produced;
        return success;
    }

    @Override
    public void onReceiversClosed() {
        receiversClosed = true;
    }

    private boolean processReceivers() throws Exception {
        int lastIdx = 0;
        int startFrom = receiversClosed ? 0 : nextReceiverIdx;
        boolean produced = false;
        for (int i = startFrom; i < receivers.length; i++) {
            lastIdx = i;
            Producer receiver = receivers[i];
            Object[] outChunk = receiver.produce();
            if (outChunk.length != 0) {
                produced = true;
                receiveBuffer.collect(outChunk, receiver.lastProducedCount());
                if (!receiverConsumerProcessor.onChunk(receiveBuffer)) {
                    nextReceiverIdx = (lastIdx + 1) % receivers.length;
                    receiversProduced = true;
                    return false;
                }
            }
        }

        receiversProduced = produced;
        nextReceiverIdx = (lastIdx + 1) % receivers.length;
        return true;
    }
}
