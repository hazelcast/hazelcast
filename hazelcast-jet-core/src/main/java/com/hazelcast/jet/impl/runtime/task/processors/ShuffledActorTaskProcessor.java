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
import com.hazelcast.jet.impl.util.JetUtil;
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

    public ShuffledActorTaskProcessor(Producer[] producers,
                                      Processor processor,
                                      TaskContext taskContext,
                                      TaskProcessor senderConsumerProcessor,
                                      TaskProcessor receiverConsumerProcessor) {
        super(producers, processor, taskContext, senderConsumerProcessor);
        this.receiverConsumerProcessor = receiverConsumerProcessor;
        List<Producer> receivers = new ArrayList<Producer>();
        JobContext jobContext = taskContext.getJobContext();
        JobManager jobManager = jobContext.getJobManager();
        VertexRunner vertexRunner = jobManager.getRunnerByVertex(taskContext.getVertex());
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskContext.getTaskNumber());

        for (Address address : jobManager.getJobContext().getSocketReaders().keySet()) {
            //Registration to the AppMaster
            ShufflingReceiver receiver = new ShufflingReceiver(vertexTask);
            jobManager.registerShufflingReceiver(taskContext.getTaskNumber(),
                    vertexRunner.getId(), address, receiver);
            receivers.add(receiver);
        }

        int chunkSize = jobContext.getJobConfig().getChunkSize();
        this.receiveBuffer = new IOBuffer<Object>(new Object[chunkSize]);
        this.receivers = receivers.toArray(new Producer[receivers.size()]);
    }

    @Override
    public void onOpen() {
        super.onOpen();

        for (Producer receiver : this.receivers) {
            receiver.open();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.receiversClosed = false;
    }

    @Override
    public boolean process() throws Exception {
        if (this.receiveBuffer.size() > 0) {
            produced = false;
            receiversProduced = false;

            boolean success = this.receiverConsumerProcessor.onChunk(this.receiveBuffer);

            if (success) {
                this.receiveBuffer.reset();
            }

            return success;
        } else if (this.outputBuffer.size() > 0) {
            return super.process();
        } else {
            boolean success;

            if (this.receivers.length > 0) {
                success = processReceivers();

                if (success) {
                    this.receiveBuffer.reset();
                }
            } else {
                receiversProduced = false;
                success = true;
            }

            success = success && super.process();

            produced = receiversProduced || produced;

            return success;
        }
    }

    @Override
    public void onReceiversClosed() {
        this.receiversClosed = true;
    }

    private boolean processReceivers() throws Exception {
        int lastIdx = 0;
        int startFrom = this.receiversClosed ? 0 : this.nextReceiverIdx;
        boolean produced = false;

        for (int i = startFrom; i < this.receivers.length; i++) {
            lastIdx = i;

            Producer receiver = this.receivers[i];
            Object[] outChunk = receiver.produce();

            if (!JetUtil.isEmpty(outChunk)) {
                produced = true;
                this.receiveBuffer.collect(outChunk, receiver.lastProducedCount());

                if (!this.receiverConsumerProcessor.onChunk(this.receiveBuffer)) {
                    this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;
                    this.receiversProduced = true;
                    return false;
                }
            }
        }

        this.receiversProduced = produced;
        this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;
        return true;
    }
}
