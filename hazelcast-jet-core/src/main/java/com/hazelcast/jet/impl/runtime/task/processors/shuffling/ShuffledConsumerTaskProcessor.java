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

package com.hazelcast.jet.impl.runtime.task.processors.shuffling;


import com.hazelcast.core.Member;
import com.hazelcast.jet.PartitionIdAware;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.ringbuffer.ShufflingSender;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.runtime.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.DataWriter;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.jet.strategy.CalculationStrategyAware;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.HashUtil;

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
    private final DataWriter[] sendersArray;
    private final boolean hasLocalConsumers;
    private final Consumer[] shuffledConsumers;
    private final Address[] nonPartitionedAddresses;
    private final Consumer[] nonPartitionedWriters;
    private final CalculationStrategy[] calculationStrategies;
    private final Map<Address, DataWriter> senders = new HashMap<Address, DataWriter>();
    private final Map<CalculationStrategy, Map<Integer, List<Consumer>>> partitionedWriters;
    private final Consumer[] markers;
    private final Map<Consumer, Integer> markersCache = new IdentityHashMap<>();
    private int lastConsumedSize;
    private boolean chunkInProgress;
    private boolean localSuccess;

    public ShuffledConsumerTaskProcessor(Consumer[] consumers,
                                         Processor processor,
                                         TaskContext taskContext) {
        this(consumers, processor, taskContext, false);
    }

    public ShuffledConsumerTaskProcessor(Consumer[] consumers,
                                         Processor processor,
                                         TaskContext taskContext,
                                         boolean receiver) {
        super(filterConsumers(consumers, false), processor, taskContext);
        this.nodeEngine = taskContext.getJobContext().getNodeEngine();

        initSenders(receiver);

        this.sendersArray = this.senders.values().toArray(new DataWriter[this.senders.size()]);
        this.hasLocalConsumers = this.consumers.length > 0;

        this.shuffledConsumers = filterConsumers(consumers, true);
        this.markers = new Consumer[this.sendersArray.length + this.shuffledConsumers.length];

        initMarkers();

        this.partitionedWriters = new HashMap<>();

        Set<CalculationStrategy> strategies = new HashSet<>();
        List<Consumer> nonPartitionedConsumers = new ArrayList<Consumer>(this.shuffledConsumers.length);
        Set<Address> nonPartitionedAddresses = new HashSet<Address>(this.shuffledConsumers.length);

        // Process distributed consumers
        initCalculationStrategies(
                strategies,
                nonPartitionedConsumers,
                nonPartitionedAddresses
        );

        this.receiver = receiver;
        this.calculationStrategies = strategies.toArray(new CalculationStrategy[strategies.size()]);
        this.nonPartitionedWriters = nonPartitionedConsumers.toArray(new Consumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
        this.chunkSize = taskContext.getJobContext().getJobConfig().getChunkSize();

        resetState();
    }

    private static Consumer[] filterConsumers(Consumer[] consumers, boolean isShuffled) {
        List<Consumer> filtered = new ArrayList<Consumer>(consumers.length);

        for (Consumer consumer : consumers) {
            if (consumer.isShuffled() == isShuffled) {
                filtered.add(consumer);
            }
        }

        return filtered.toArray(new Consumer[filtered.size()]);
    }


    private void initMarkers() {
        int position = 0;

        for (DataWriter sender : this.sendersArray) {
            this.markersCache.put(sender, position++);
        }
        for (Consumer shuffledConsumer : this.shuffledConsumers) {
            this.markersCache.put(shuffledConsumer, position++);
        }
    }

    private void initSenders(boolean receiver) {
        JobManager jobManager = taskContext.getJobContext().getJobManager();

        VertexRunner vertexRunner = jobManager.getRunnerByVertex(taskContext.getVertex());
        int taskNumber = taskContext.getTaskNumber();
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskNumber);

        if (!receiver) {
            for (Address address : jobManager.getJobContext().getSocketWriters().keySet()) {
                ShufflingSender sender = new ShufflingSender(vertexTask, vertexRunner.getId(), address);
                this.senders.put(address, sender);
                jobManager.registerShufflingSender(taskNumber, vertexRunner.getId(), address, sender);
            }
        }
    }

    private void initCalculationStrategies(Set<CalculationStrategy> strategies,
                                           List<Consumer> nonPartitionedConsumers,
                                           Set<Address> nonPartitionedAddresses) {

        JobManager jobManager = taskContext.getJobContext().getJobManager();
        Map<Address, Address> hzToJetAddressMapping = jobManager.getJobContext().getHzToJetAddressMapping();
        List<Integer> localPartitions = JetUtil.getLocalPartitions(nodeEngine);
        for (Consumer consumer : this.shuffledConsumers) {
            MemberDistributionStrategy memberDistributionStrategy = consumer.getMemberDistributionStrategy();
            initConsumerCalculationStrategy(
                    strategies,
                    localPartitions,
                    nonPartitionedConsumers,
                    nonPartitionedAddresses,
                    hzToJetAddressMapping,
                    consumer,
                    memberDistributionStrategy
            );
        }
    }

    private void initConsumerCalculationStrategy(Set<CalculationStrategy> strategies,
                                                 List<Integer> localPartitions,
                                                 List<Consumer> nonPartitionedConsumers,
                                                 Set<Address> nonPartitionedAddresses,
                                                 Map<Address, Address> hzToJetAddressMapping,
                                                 Consumer consumer,
                                                 MemberDistributionStrategy memberDistributionStrategy) {
        if (memberDistributionStrategy != null) {
            Set<Member> members = new HashSet<>(memberDistributionStrategy.getTargetMembers(taskContext.getJobContext()));

            if (members != null) {
                for (Member member : members) {
                    Address address = member.getAddress();
                    if (address.equals(nodeEngine.getThisAddress())) {
                        nonPartitionedConsumers.add(
                                consumer
                        );
                    } else {
                        nonPartitionedAddresses.add(
                                hzToJetAddressMapping.get(address)
                        );
                    }
                }

                return;
            }
        }

        initConsumerPartitions(
                strategies,
                localPartitions,
                nonPartitionedConsumers,
                nonPartitionedAddresses,
                hzToJetAddressMapping,
                consumer
        );
    }

    private void initConsumerPartitions(Set<CalculationStrategy> strategies,
                                        List<Integer> localPartitions,
                                        List<Consumer> nonPartitionedConsumers,
                                        Set<Address> nonPartitionedAddresses,
                                        Map<Address, Address> hzToJetAddressMapping,
                                        Consumer consumer
    ) {
        CalculationStrategy calculationStrategy = new CalculationStrategy(
                consumer.getHashingStrategy(),
                consumer.getPartitionStrategy(),
                taskContext.getJobContext()
        );

        int partitionId;
        if (consumer instanceof DataWriter) {
            DataWriter writer = (DataWriter) consumer;

            if (!writer.isPartitioned()) {
                if (writer.getPartitionId() >= 0) {
                    processWriterPartition(
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

                return;
            }

            partitionId = writer.getPartitionId();
        } else {
            partitionId = -1;
        }

        strategies.add(calculationStrategy);
        Map<Integer, List<Consumer>> map = this.partitionedWriters.get(calculationStrategy);

        if (map == null) {
            map = new HashMap<>();
            this.partitionedWriters.put(calculationStrategy, map);
        }

        if (partitionId >= 0) {
            processPartition(map, partitionId).add(consumer);
        } else {
            for (Integer localPartition : localPartitions) {
                processPartition(map, localPartition).add(consumer);
            }
        }
    }

    private void processWriterPartition(List<Consumer> nonPartitionedConsumers,
                                        Set<Address> nonPartitionedAddresses,
                                        Map<Address, Address> hzToJetAddressMapping,
                                        DataWriter writer) {
        if (JetUtil.isPartitionLocal(nodeEngine, writer.getPartitionId())) {
            nonPartitionedConsumers.add(writer);
        } else {
            nonPartitionedAddresses.add(
                    hzToJetAddressMapping.get(
                            this.nodeEngine.getPartitionService().getPartitionOwner(writer.getPartitionId())
                    )
            );
        }
    }

    private List<Consumer> processPartition(Map<Integer, List<Consumer>> map, int partitionId) {
        List<Consumer> partitionOwnerWriters = map.get(partitionId);

        if (partitionOwnerWriters == null) {
            partitionOwnerWriters = new ArrayList<>();
            map.put(partitionId, partitionOwnerWriters);
        }

        return partitionOwnerWriters;
    }

    public void onOpen() {
        super.onOpen();

        for (Consumer consumer : this.shuffledConsumers) {
            consumer.open();
        }

        for (DataWriter pairWriter : this.sendersArray) {
            pairWriter.open();
        }

        reset();
    }

    public void onClose() {
        super.onClose();

        for (Consumer consumer : this.shuffledConsumers) {
            consumer.close();
        }

        for (DataWriter pairWriter : this.sendersArray) {
            pairWriter.close();
        }
    }

    private void resetState() {
        this.lastConsumedSize = 0;
        this.chunkInProgress = false;
        this.localSuccess = this.receiver || !this.hasLocalConsumers;
    }

    @Override
    public boolean onChunk(InputChunk<Object> inputChunk) throws Exception {
        if (inputChunk.size() > 0) {
            boolean success = false;
            boolean consumed;

            /// Local consumers
            consumed = processLocalConsumers(inputChunk);

            // Shufflers
            boolean chunkPooled = this.lastConsumedSize >= inputChunk.size();

            if ((!chunkInProgress) && (!chunkPooled)) {
                this.lastConsumedSize = processShufflers(inputChunk, this.lastConsumedSize);
                chunkPooled = this.lastConsumedSize >= inputChunk.size();
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
                    (this.localSuccess)) {
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

    private boolean processLocalConsumers(InputChunk<Object> chunk) throws Exception {
        boolean consumed = false;

        if (!this.receiver) {
            if ((this.hasLocalConsumers) && (!this.localSuccess)) {
                this.localSuccess = super.onChunk(chunk);
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

        for (Consumer consumer : this.shuffledConsumers) {
            flushed &= consumer.isFlushed();
        }

        return flushed;
    }

    private int processShufflers(InputChunk<Object> chunk, int lastConsumedSize) throws Exception {
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
        for (Consumer consumer : this.shuffledConsumers) {
            consumer.flush();
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
        Map<Integer, List<Consumer>> cache = this.partitionedWriters.get(calculationStrategy);
        List<Consumer> writers = null;

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
                objectPartitionId = calculatePartitionId(object, calculationStrategy);
                writers = cache.get(objectPartitionId);
            }
        } else {
            //Send to another node
            objectPartitionId = calculatePartitionId(object, calculationStrategy);
        }

        Address address = this.nodeEngine.getPartitionService().getPartitionOwner(objectPartitionId);
        Address remoteJetAddress = taskContext.getJobContext().getHzToJetAddressMapping().get(address);
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

    private int calculatePartitionId(Object object, CalculationStrategy calculationStrategy) {
        return HashUtil.hashToIndex(calculationStrategy.hash(object),
                this.nodeEngine.getPartitionService().getPartitionCount());
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
        for (Consumer marker : this.markers) {
            if (marker != null) {
                marker.consume(object);
            }
        }
    }

    private void markConsumer(Consumer consumer) {
        int position = this.markersCache.get(consumer);
        this.markers[position] = consumer;
    }

    private void writeToNonPartitionedLocals(InputChunk<Object> chunk) throws Exception {
        for (Consumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consume(chunk);
        }
    }

    private void sendToNonPartitionedRemotes(InputChunk<Object> chunk) throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            this.senders.get(remoteAddress).consume(chunk);
        }
    }

    private void writeToNonPartitionedLocals(Object object) throws Exception {
        for (Consumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consume(object);
        }
    }

    private void markNonPartitionedRemotes() throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            markConsumer(this.senders.get(remoteAddress));
        }
    }
}
