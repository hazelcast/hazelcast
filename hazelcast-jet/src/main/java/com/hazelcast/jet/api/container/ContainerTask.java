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

package com.hazelcast.jet.api.container;

import java.util.List;

import com.hazelcast.nio.Address;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.jet.api.actor.ComposedActor;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.spi.executor.TaskContext;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;

/**
 * Interface which represents container's execution task;
 */
public interface ContainerTask extends Task {
    /**
     * @return - corresponding DAG's vertex;
     */
    Vertex getVertex();

    /**
     * Start finalization of the task;
     */
    void startFinalization();

    /**
     * Handled on input producer's completion;
     *
     * @param producer - finished input producer;
     */
    void handleProducerCompleted(ObjectProducer producer);

    /**
     * Start tasks' execution;
     * Initialize initial state of the task;
     *
     * @param producers - list of the input producers;
     */
    void start(List<? extends ObjectProducer> producers);

    /**
     * Performs registration of sink writers;
     *
     * @param sinkWriters - list of the input sink writers;
     */
    void registerSinkWriters(List<DataWriter> sinkWriters);

    /**
     * @param endPoint - jet-Address of the corresponding shuffling-receiver;
     * @return - corresponding shuffling-receiver;
     */
    ShufflingReceiver getShufflingReceiver(Address endPoint);

    /**
     * Register shuffling sender for the corresponding node with address member;
     *
     * @param member - member's address;
     * @param sender - corresponding shuffling sender;
     */
    void registerShufflingSender(Address member, ShufflingSender sender);

    /**
     * Register shuffling receiver for the corresponding node with address member;
     *
     * @param member   - member's address;
     * @param receiver - corresponding shuffling receiver;
     */
    void registerShufflingReceiver(Address member, ShufflingReceiver receiver);

    /**
     * @return - task context;
     */
    TaskContext getTaskContext();

    /**
     * @param channel         - data channel for corresponding edge;
     * @param edge            - corresponding edge;
     * @param targetContainer - source container of the channel;
     * @return - composed actor with actors of channel;
     */
    ComposedActor registerOutputChannel(DataChannel channel, Edge edge, ProcessingContainer targetContainer);
}
