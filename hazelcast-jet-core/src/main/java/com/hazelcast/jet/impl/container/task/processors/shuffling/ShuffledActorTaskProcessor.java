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

package com.hazelcast.jet.impl.container.task.processors.shuffling;

import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.container.task.processors.ActorTaskProcessor;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.List;

public class ShuffledActorTaskProcessor extends ActorTaskProcessor {
    private final ObjectProducer[] receivers;
    private final DefaultObjectIOStream<Object> receivedTupleStream;
    private final TaskProcessor receiverConsumerProcessor;
    private int nextReceiverIdx;
    private boolean receiversClosed;
    private boolean receiversProduced;

    public ShuffledActorTaskProcessor(ObjectProducer[] producers,
                                      ObjectConsumer[] consumers,
                                      ContainerProcessor processor,
                                      ContainerContext containerContext,
                                      ProcessorContext processorContext,
                                      TaskProcessor senderConsumerProcessor,
                                      TaskProcessor receiverConsumerProcessor,
                                      int taskID) {
        super(producers, processor, containerContext, processorContext, senderConsumerProcessor, taskID);
        this.receiverConsumerProcessor = receiverConsumerProcessor;
        List<ObjectProducer> receivers = new ArrayList<ObjectProducer>();
        ApplicationMaster applicationMaster = containerContext.getApplicationContext().getApplicationMaster();
        ProcessingContainer processingContainer = applicationMaster.getContainerByVertex(containerContext.getVertex());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);

        for (Address address : applicationMaster.getApplicationContext().getSocketReaders().keySet()) {
            //Registration to the AppMaster
            ShufflingReceiver receiver = new ShufflingReceiver(containerContext, containerTask, address);
            applicationMaster.registerShufflingReceiver(taskID, containerContext, address, receiver);
            receivers.add(receiver);
        }

        int chunkSize = containerContext.getApplicationContext().getJetApplicationConfig().getChunkSize();
        this.receivedTupleStream = new DefaultObjectIOStream<Object>(new Object[chunkSize]);
        this.receivers = receivers.toArray(new ObjectProducer[receivers.size()]);
    }

    @Override
    public void onOpen() {
        super.onOpen();

        for (ObjectProducer receiver : this.receivers) {
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
        if (this.receivedTupleStream.size() > 0) {
            produced = false;
            receiversProduced = false;

            boolean success = this.receiverConsumerProcessor.onChunk(this.receivedTupleStream);

            if (success) {
                this.receivedTupleStream.reset();
            }

            return success;
        } else if (this.tupleOutputStream.size() > 0) {
            return super.process();
        } else {
            boolean success;

            if (this.receivers.length > 0) {
                success = processReceivers();

                if (success) {
                    this.receivedTupleStream.reset();
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

            ObjectProducer receiver = this.receivers[i];
            Object[] outChunk = receiver.produce();

            if (!JetUtil.isEmpty(outChunk)) {
                produced = true;
                this.receivedTupleStream.consumeChunk(outChunk, receiver.lastProducedCount());

                if (!this.receiverConsumerProcessor.onChunk(this.receivedTupleStream)) {
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
