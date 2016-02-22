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
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.container.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.PartitionIdAware;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.jet.spi.strategy.CalculationStrategyAware;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.IPartition;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ShuffledConsumerTaskProcessor extends ConsumerTaskProcessor {
    private final int chunkSize;
    private final boolean receiver;
    private final NodeEngine nodeEngine;
    private final boolean hasActiveConsumers;
    private final DataWriter[] sendersArray;
    private final boolean hasUnShuffledConsumers;
    private final ObjectConsumer[] shuffledConsumers;
    private final Address[] nonPartitionedAddresses;
    private final ObjectConsumer[] nonPartitionedWriters;
    private final CalculationStrategy[] calculationStrategies;
    private final Map<Address, DataWriter> senders = new HashMap<Address, DataWriter>();
    private final Map<CalculationStrategy, Map<Integer, List<ObjectConsumer>>> partitionedWriters;
    private final ObjectConsumer[] markers;
    private final Map<ObjectConsumer, Integer> markersCache = new IdentityHashMap<ObjectConsumer, Integer>();
    private final ContainerContext containerContext;
    private int lastConsumedSize;
    private boolean chunkInProgress;
    private boolean unShufflersSuccess;

    public ShuffledConsumerTaskProcessor(ObjectConsumer[] consumers,
                                         ContainerProcessor processor,
                                         ContainerContext containerContext,
                                         ProcessorContext processorContext,
                                         int taskID) {
        this(consumers, processor, containerContext, processorContext, taskID, false);
    }

    public ShuffledConsumerTaskProcessor(ObjectConsumer[] consumers,
                                         ContainerProcessor processor,
                                         ContainerContext containerContext,
                                         ProcessorContext processorContext,
                                         int taskID,
                                         boolean receiver) {
        super(filterConsumers(consumers, false), processor, containerContext, processorContext);
        this.nodeEngine = containerContext.getNodeEngine();
        this.containerContext = containerContext;

        initSenders(containerContext, taskID, receiver);

        this.sendersArray = this.senders.values().toArray(new DataWriter[this.senders.size()]);
        this.hasUnShuffledConsumers = this.consumers.length > 0;

        this.shuffledConsumers = filterConsumers(consumers, true);
        this.markers = new ObjectConsumer[this.sendersArray.length + this.shuffledConsumers.length];

        initMarkers();

        this.partitionedWriters = new HashMap<CalculationStrategy, Map<Integer, List<ObjectConsumer>>>();

        Set<CalculationStrategy> strategies = new HashSet<CalculationStrategy>();
        List<ObjectConsumer> nonPartitionedConsumers = new ArrayList<ObjectConsumer>(this.shuffledConsumers.length);
        Set<Address> nonPartitionedAddresses = new HashSet<Address>(this.shuffledConsumers.length);

        // Process shuffled consumers
        this.hasActiveConsumers = initCalculationStrategies(
                strategies,
                nonPartitionedConsumers,
                nonPartitionedAddresses
        );

        this.receiver = receiver;
        this.calculationStrategies = strategies.toArray(new CalculationStrategy[strategies.size()]);
        this.nonPartitionedWriters = nonPartitionedConsumers.toArray(new ObjectConsumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
        this.chunkSize = chunkSize(containerContext);
        resetState();
    }

    private static ObjectConsumer[] filterConsumers(ObjectConsumer[] consumers, boolean isShuffled) {
        List<ObjectConsumer> filtered = new ArrayList<ObjectConsumer>(consumers.length);

        for (ObjectConsumer consumer : consumers) {
            if (consumer.isShuffled() == isShuffled) {
                filtered.add(consumer);
            }
        }

        return filtered.toArray(new ObjectConsumer[filtered.size()]);
    }

    private int chunkSize(ContainerContext containerContext) {
        JetApplicationConfig jetApplicationConfig = containerContext.getApplicationContext().getJetApplicationConfig();
        return jetApplicationConfig.getChunkSize();
    }

    private void initMarkers() {
        int position = 0;

        for (DataWriter sender : this.sendersArray) {
            this.markersCache.put(sender, position++);
        }
        for (ObjectConsumer shuffledConsumer : this.shuffledConsumers) {
            this.markersCache.put(shuffledConsumer, position++);
        }
    }

    private void initSenders(ContainerContext containerContext, int taskID, boolean receiver) {
        ApplicationMaster applicationMaster = containerContext.getApplicationContext().getApplicationMaster();

        ProcessingContainer processingContainer = applicationMaster.getContainerByVertex(containerContext.getVertex());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);

        if (!receiver) {
            for (Address address : applicationMaster.getApplicationContext().getSocketWriters().keySet()) {
                ShufflingSender sender = new ShufflingSender(containerContext, taskID, containerTask, address);
                this.senders.put(address, sender);
                applicationMaster.registerShufflingSender(taskID, containerContext, address, sender);
            }
        }

        if (!this.hasUnShuffledConsumers) {
            this.unShufflersSuccess = true;
        }
    }

    private boolean initCalculationStrategies(Set<CalculationStrategy> strategies,
                                              List<ObjectConsumer> nonPartitionedConsumers,
                                              Set<Address> nonPartitionedAddresses) {
        List<IPartition> localPartitions = new ArrayList<IPartition>();

        ApplicationMaster applicationMaster = this.containerContext.getApplicationContext().getApplicationMaster();
        Map<Address, Address> hzToJetAddressMapping = applicationMaster.getApplicationContext().getHzToJetAddressMapping();
        boolean hasActiveConsumers = false;

        for (IPartition partition : this.nodeEngine.getPartitionService().getPartitions()) {
            if (partition.isLocal()) {
                localPartitions.add(partition);
            }
        }

        for (ObjectConsumer consumer : this.shuffledConsumers) {
            ShufflingStrategy shufflingStrategy = consumer.getShufflingStrategy();
            hasActiveConsumers = initConsumerCalculationStrategy(
                    strategies,
                    localPartitions,
                    nonPartitionedConsumers,
                    nonPartitionedAddresses,
                    hasActiveConsumers,
                    hzToJetAddressMapping,
                    consumer,
                    shufflingStrategy);
        }

        return hasActiveConsumers;
    }

    private boolean initConsumerCalculationStrategy(Set<CalculationStrategy> strategies,
                                                    List<IPartition> localPartitions,
                                                    List<ObjectConsumer> nonPartitionedConsumers,
                                                    Set<Address> nonPartitionedAddresses,
                                                    boolean hasActiveConsumers,
                                                    Map<Address, Address> hzToJetAddressMapping,
                                                    ObjectConsumer consumer,
                                                    ShufflingStrategy shufflingStrategy) {
        if (shufflingStrategy != null) {
            Address[] addresses = shufflingStrategy.getShufflingAddress(this.containerContext);

            if (addresses != null) {
                for (Address address : addresses) {
                    if (address.equals(this.nodeEngine.getThisAddress())) {
                        hasActiveConsumers = true;
                        nonPartitionedConsumers.add(
                                consumer
                        );
                    } else {
                        nonPartitionedAddresses.add(
                                hzToJetAddressMapping.get(address)
                        );
                    }
                }

                return hasActiveConsumers;
            }
        }

        hasActiveConsumers = initConsumerPartitions(
                strategies,
                localPartitions,
                nonPartitionedConsumers,
                nonPartitionedAddresses,
                hzToJetAddressMapping,
                consumer
        );

        return hasActiveConsumers;
    }

    private boolean initConsumerPartitions(Set<CalculationStrategy> strategies,
                                           List<IPartition> localPartitions,
                                           List<ObjectConsumer> nonPartitionedConsumers,
                                           Set<Address> nonPartitionedAddresses,
                                           Map<Address, Address> hzToJetAddressMapping, ObjectConsumer consumer
    ) {
        CalculationStrategy calculationStrategy = new CalculationStrategyImpl(
                consumer.getHashingStrategy(),
                consumer.getPartitionStrategy(),
                this.containerContext
        );

        boolean hasActiveConsumers = false;
        int partitionId;
        if (consumer instanceof DataWriter) {
            DataWriter writer = (DataWriter) consumer;

            if (!writer.isPartitioned()) {
                if (writer.getPartitionId() >= 0) {
                    hasActiveConsumers = processWriterPartition(
                            nonPartitionedConsumers,
                            nonPartitionedAddresses,
                            hzToJetAddressMapping,
                            writer
                    );
                } else {
                    nonPartitionedConsumers.add(
                            writer
                    );
                }

                return hasActiveConsumers;
            }

            partitionId = writer.getPartitionId();
        } else {
            partitionId = -1;
        }

        strategies.add(calculationStrategy);
        Map<Integer, List<ObjectConsumer>> map = this.partitionedWriters.get(calculationStrategy);

        if (map == null) {
            map = new HashMap<Integer, List<ObjectConsumer>>();
            this.partitionedWriters.put(calculationStrategy, map);
        }

        if (partitionId >= 0) {
            processPartition(map, partitionId).add(consumer);
        } else {
            for (IPartition localPartition : localPartitions) {
                processPartition(map, localPartition.getPartitionId()).add(consumer);
            }
        }
        return true;
    }

    private boolean processWriterPartition(List<ObjectConsumer> nonPartitionedConsumers,
                                           Set<Address> nonPartitionedAddresses,
                                           Map<Address, Address> hzToJetAddressMapping,
                                           DataWriter writer) {
        boolean hasActiveConsumers = false;

        IPartition partition = this.nodeEngine.getPartitionService().getPartition(writer.getPartitionId());

        if (partition.isLocal()) {
            hasActiveConsumers = true;
            nonPartitionedConsumers.add(
                    writer
            );
        } else {
            nonPartitionedAddresses.add(
                    hzToJetAddressMapping.get(
                            this.nodeEngine.getPartitionService().getPartitionOwner(partition.getPartitionId())
                    )
            );
        }
        return hasActiveConsumers;
    }

    private List<ObjectConsumer> processPartition(Map<Integer, List<ObjectConsumer>> map, int partitionId) {
        List<ObjectConsumer> partitionOwnerWriters = map.get(partitionId);

        if (partitionOwnerWriters == null) {
            partitionOwnerWriters = new ArrayList<ObjectConsumer>();
            map.put(partitionId, partitionOwnerWriters);
        }

        return partitionOwnerWriters;
    }

    public void onOpen() {
        super.onOpen();

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.open();
        }

        for (DataWriter tupleWriter : this.sendersArray) {
            tupleWriter.open();
        }

        reset();
    }

    public void onClose() {
        super.onClose();

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.close();
        }

        for (DataWriter tupleWriter : this.sendersArray) {
            tupleWriter.close();
        }
    }

    private void resetState() {
        this.lastConsumedSize = 0;
        this.chunkInProgress = false;
        this.unShufflersSuccess = !this.hasUnShuffledConsumers;
    }

    @Override
    public boolean onChunk(ProducerInputStream<Object> chunk) throws Exception {
        if (chunk.size() > 0) {
            boolean success = false;
            boolean consumed;

            /// UnShufflers
            consumed = processUnShufflers(chunk);

            // Shufflers
            boolean chunkPooled = this.lastConsumedSize >= chunk.size();

            if ((!chunkInProgress) && (!chunkPooled)) {
                this.lastConsumedSize = processShufflers(chunk, this.lastConsumedSize);
                chunkPooled = this.lastConsumedSize >= chunk.size();
                this.chunkInProgress = true;
                consumed = true;
            }

            if (this.chunkInProgress) {
                boolean flushed = processChunkProgress();
                consumed = true;

                if (flushed) {
                    this.chunkInProgress = false;
                }
            }

            /// Check if we are success with chunk
            if ((!this.chunkInProgress)
                    &&
                    (chunkPooled)
                    &&
                    (this.unShufflersSuccess)) {
                success = true;
                consumed = true;
            }

            if (success) {
                resetState();
            }

            this.consumed = consumed;
            return success;
        } else {
            this.consumed = false;
            return true;
        }
    }

    private boolean processUnShufflers(ProducerInputStream<Object> chunk) throws Exception {
        boolean consumed = false;

        if (!this.receiver) {
            if ((this.hasUnShuffledConsumers) && (!this.unShufflersSuccess)) {
                this.unShufflersSuccess = super.onChunk(chunk);
                consumed = super.consumed();
            }
        }
        return consumed;
    }

    private boolean processChunkProgress() {
        boolean flushed = true;

        for (DataWriter sender : this.sendersArray) {
            flushed &= sender.isFlushed();
        }

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            flushed &= objectConsumer.isFlushed();
        }

        return flushed;
    }

    private int processShufflers(ProducerInputStream<Object> chunk, int lastConsumedSize) throws Exception {
        if (this.calculationStrategies.length > 0) {
            int toIdx = Math.min(lastConsumedSize + this.chunkSize, chunk.size());
            int consumedSize = 0;

            for (int i = lastConsumedSize; i < toIdx; i++) {
                Object object = chunk.get(i);

                consumedSize++;
                processCalculationStrategies(object);
            }

            flush();
            return lastConsumedSize + consumedSize;
        } else {
            writeToNonPartitionedLocals(chunk);

            if (!this.receiver) {
                sendToNonPartitionedRemotes(chunk);
            }

            flush();
            return chunk.size();
        }
    }

    private void flush() {
        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.flush();
        }

        if (!this.receiver) {
            for (DataWriter sender : this.sendersArray) {
                sender.flush();
            }
        }
    }

    private boolean processCalculationStrategy(
            Object object,
            CalculationStrategy calculationStrategy,
            int objectPartitionId,
            CalculationStrategy objectCalculationStrategy,
            boolean markedNonPartitionedRemotes
    ) throws Exception {
        Map<Integer, List<ObjectConsumer>> cache = this.partitionedWriters.get(calculationStrategy);
        List<ObjectConsumer> writers = null;

        if (cache.size() > 0) {
            if (
                    (objectCalculationStrategy != null)
                            &&
                            (objectCalculationStrategy.equals(calculationStrategy))
                            &&
                            (objectPartitionId >= 0)
                    ) {
                writers = cache.get(objectPartitionId);
            } else {
                objectPartitionId = calculationStrategy.hash(object) % this.nodeEngine.getPartitionService().getPartitionCount();
                writers = cache.get(objectPartitionId);
            }
        } else {
            //Send to another node
            objectPartitionId = calculationStrategy.hash(object) % this.nodeEngine.getPartitionService().getPartitionCount();
        }

        Address address = this.nodeEngine.getPartitionService().getPartitionOwner(objectPartitionId);
        Address remoteJetAddress = containerContext.getApplicationContext().getHzToJetAddressMapping().get(address);
        DataWriter sender = this.senders.get(remoteJetAddress);

        if (sender != null) {
            markConsumer(sender);

            if (!markedNonPartitionedRemotes) {
                for (Address remoteAddress : this.nonPartitionedAddresses) {
                    if (!remoteAddress.equals(address)) {
                        markConsumer(this.senders.get(remoteAddress));
                    }
                }
            }
        } else if (!JetUtil.isEmpty(writers)) {
            //Write to partitioned locals
            for (int ir = 0; ir < writers.size(); ir++) {
                markConsumer(writers.get(ir));
            }

            if (!markedNonPartitionedRemotes) {
                markNonPartitionedRemotes();
                markedNonPartitionedRemotes = true;
            }
        }

        return markedNonPartitionedRemotes;
    }


    private void processCalculationStrategies(Object object) throws Exception {
        CalculationStrategy objectCalculationStrategy = null;
        int objectPartitionId = -1;

        if (object instanceof CalculationStrategyAware) {
            CalculationStrategyAware calculationStrategyAware = ((CalculationStrategyAware) object);
            objectCalculationStrategy = calculationStrategyAware.getCalculationStrategy();
        }

        if (object instanceof PartitionIdAware) {
            objectPartitionId = ((PartitionIdAware) object).getPartitionId();
        }

        writeToNonPartitionedLocals(object);

        Arrays.fill(this.markers, null);

        boolean markedNonPartitionedRemotes = false;

        for (CalculationStrategy calculationStrategy : this.calculationStrategies) {
            markedNonPartitionedRemotes = processCalculationStrategy(
                    object,
                    calculationStrategy,
                    objectPartitionId,
                    objectCalculationStrategy,
                    markedNonPartitionedRemotes
            );
        }

        sendToMarked(object);
    }

    private void sendToMarked(Object object) throws Exception {
        for (ObjectConsumer marker : this.markers) {
            if (marker != null) {
                marker.consumeObject(object);
            }
        }
    }

    private void markConsumer(ObjectConsumer consumer) {
        int position = this.markersCache.get(consumer);
        this.markers[position] = consumer;
    }

    @Override
    public boolean hasActiveConsumers() {
        return this.hasActiveConsumers;
    }

    private void writeToNonPartitionedLocals(ProducerInputStream<Object> chunk) throws Exception {
        for (ObjectConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeChunk(chunk);
        }
    }

    private void sendToNonPartitionedRemotes(ProducerInputStream<Object> chunk) throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            this.senders.get(remoteAddress).consumeChunk(chunk);
        }
    }

    private void writeToNonPartitionedLocals(Object object) throws Exception {
        for (ObjectConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeObject(object);
        }
    }

    private void markNonPartitionedRemotes() throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            markConsumer(this.senders.get(remoteAddress));
        }
    }
}
