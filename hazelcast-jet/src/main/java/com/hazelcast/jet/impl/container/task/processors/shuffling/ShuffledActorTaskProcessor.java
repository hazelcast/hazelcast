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

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.nio.Address;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.data.DataReader;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.container.task.processors.ActorTaskProcessor;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

public class ShuffledActorTaskProcessor extends ActorTaskProcessor {
    private int nextReceiverIdx;

    private final ObjectProducer[] receivers;
    private final DefaultObjectIOStream<Object> receivedTupleStream;
    private final TaskProcessor receiverConsumerProcessor;

    private final boolean hasActiveProducers;

    private boolean receiversClosed;

    public ShuffledActorTaskProcessor(ObjectProducer[] producers,
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

        this.hasActiveProducers = defineHasActiveChannels(producers, containerContext);
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

    private boolean defineHasActiveChannels(ObjectProducer[] producers, ContainerContext containerContext) {
        boolean hasActiveChannels = false;

        //Check if we will receive objects from input channels
        for (ObjectProducer producer : producers) {
            if (producer instanceof DataReader) {
                hasActiveChannels = true;
            }
        }

        for (Edge edge : containerContext.getVertex().getInputEdges()) {
            if (edge.getShufflingStrategy() == null) {
                hasActiveChannels = true;
            } else {
                Address[] shufflingAddresses = edge.getShufflingStrategy().getShufflingAddress(containerContext);

                if (shufflingAddresses != null) {
                    if (searchAddress(shufflingAddresses, containerContext.getNodeEngine().getThisAddress())) {
                        hasActiveChannels = true;
                    }
                } else {
                    hasActiveChannels = true;
                }
            }
        }
        return hasActiveChannels;
    }


    private boolean searchAddress(Address[] addresses, Address address) {
        for (Address addr : addresses) {
            if (addr.equals(address)) {
                return true;
            }
        }

        return false;
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
                success = true;
            }

            success = success && super.process();
            return success;
        }
    }

    @Override
    public void onReceiversClosed() {
        this.receiversClosed = true;
    }

    @Override
    public boolean hasActiveProducers() {
        return this.hasActiveProducers;
    }

    private boolean processReceivers() throws Exception {
        int lastIdx = 0;
        int startFrom = this.receiversClosed ? 0 : this.nextReceiverIdx;

        for (int i = startFrom; i < this.receivers.length; i++) {
            lastIdx = i;

            ObjectProducer receiver = this.receivers[i];
            Object[] outChunk = receiver.produce();

            if (!JetUtil.isEmpty(outChunk)) {
                this.produced = true;
                this.receivedTupleStream.consumeChunk(outChunk, receiver.lastProducedCount());

                if (!this.receiverConsumerProcessor.onChunk(this.receivedTupleStream)) {
                    this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;
                    return false;
                }
            }
        }

        this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;
        return true;
    }
}
