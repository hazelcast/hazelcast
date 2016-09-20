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


import com.hazelcast.core.Member;
import com.hazelcast.jet.PartitionIdAware;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.ringbuffer.ShufflingSender;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.runtime.Consumer;
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

import static com.hazelcast.jet.impl.util.JetUtil.isPartitionLocal;
import static java.util.stream.Collectors.toList;


public class ShuffledConsumerTaskProcessor extends ConsumerTaskProcessor {
    private final int chunkSize;
    private final boolean isReceiver;
    private final boolean hasLocalConsumers;
    private final Consumer[] senders;
    private final Consumer[] shuffledConsumers;
    private final Address[] nonPartitionedAddresses;
    private final Consumer[] nonPartitionedWriters;
    private final Consumer[] markers;
    private final CalculationStrategy[] calculationStrategies;
    private final Map<Address, Consumer> addrToSender = new HashMap<>();
    private final Map<CalculationStrategy, Map<Integer, List<Consumer>>> partitionedConsumers;
    private final Map<Consumer, Integer> markersCache = new IdentityHashMap<>();
    private final NodeEngine nodeEngine;

    private int lastConsumedSize;
    private boolean chunkInProgress;
    private boolean localSuccess;

    public ShuffledConsumerTaskProcessor(
            Consumer[] consumers, Processor processor, TaskContext taskContext, boolean isReceiver) {
        super(filterConsumers(consumers, false), processor, taskContext);
        this.nodeEngine = taskContext.getJobContext().getNodeEngine();
        if (!isReceiver) {
            initSenders();
        }
        this.senders = this.addrToSender.values().toArray(new Consumer[this.addrToSender.size()]);
        this.hasLocalConsumers = this.consumers.length > 0;
        this.shuffledConsumers = filterConsumers(consumers, true);
        this.markers = new Consumer[this.senders.length + this.shuffledConsumers.length];
        initMarkers();
        this.partitionedConsumers = new HashMap<>();
        Set<CalculationStrategy> strategies = new HashSet<>();
        List<Consumer> nonPartitionedConsumers = new ArrayList<>(this.shuffledConsumers.length);
        Set<Address> nonPartitionedAddresses = new HashSet<>(this.shuffledConsumers.length);
        initCalculationStrategies(strategies, nonPartitionedConsumers, nonPartitionedAddresses);
        this.isReceiver = isReceiver;
        this.calculationStrategies = strategies.toArray(new CalculationStrategy[strategies.size()]);
        this.nonPartitionedWriters = nonPartitionedConsumers.toArray(new Consumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
        this.chunkSize = taskContext.getJobContext().getJobConfig().getChunkSize();
        resetState();
    }

    @Override
    public void onOpen() {
        super.onOpen();
        for (Consumer consumer : shuffledConsumers) {
            consumer.open();
        }
        for (Consumer sender : senders) {
            sender.open();
        }
        reset();
    }

    @Override
    public void onClose() {
        super.onClose();
        for (Consumer consumer : shuffledConsumers) {
            consumer.close();
        }
        for (Consumer sender : senders) {
            sender.close();
        }
    }
    @Override
    public boolean onChunk(InputChunk<Object> inputChunk) throws Exception {
        if (inputChunk.size() == 0) {
            consumedSome = false;
            return true;
        }
        boolean success = false;
        boolean consumed = processLocalConsumers(inputChunk);
        boolean chunkPooled = lastConsumedSize >= inputChunk.size();
        if (!chunkInProgress && !chunkPooled) {
            lastConsumedSize = processShufflers(inputChunk, lastConsumedSize);
            chunkPooled = lastConsumedSize >= inputChunk.size();
            chunkInProgress = true;
            consumed = true;
        }
        if (chunkInProgress) {
            boolean flushed = processChunkProgress();
            consumed = true;
            if (flushed) {
                chunkInProgress = false;
            }
        }
        if (!chunkInProgress && chunkPooled && localSuccess) {
            success = true;
            consumed = true;
        }
        if (success) {
            resetState();
        }
        this.consumedSome = consumed;
        return success;
    }

    private static Consumer[] filterConsumers(Consumer[] consumers, boolean isShuffled) {
        final List<Consumer> filtered = Arrays.stream(consumers)
                .filter(c -> c.isShuffled() == isShuffled).collect(toList());
        return filtered.toArray(new Consumer[filtered.size()]);
    }


    private static List<Consumer> processPartition(Map<Integer, List<Consumer>> map, int partitionId) {
        List<Consumer> partitionOwnerWriters = map.get(partitionId);
        if (partitionOwnerWriters == null) {
            partitionOwnerWriters = new ArrayList<>();
            map.put(partitionId, partitionOwnerWriters);
        }
        return partitionOwnerWriters;
    }

    private void initMarkers() {
        int position = 0;
        for (Consumer sender : senders) {
            markersCache.put(sender, position++);
        }
        for (Consumer shuffledConsumer : shuffledConsumers) {
            markersCache.put(shuffledConsumer, position++);
        }
    }

    private void initSenders() {
        JobManager jobManager = taskContext.getJobContext().getJobManager();
        VertexRunner vertexRunner = jobManager.getRunnerByVertex(taskContext.getVertex());
        int taskNumber = taskContext.getTaskNumber();
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskNumber);
        for (Address address : jobManager.getJobContext().getSocketWriters().keySet()) {
            ShufflingSender sender = new ShufflingSender(vertexTask, vertexRunner.getId());
            addrToSender.put(address, sender);
            jobManager.registerShufflingSender(taskNumber, vertexRunner.getId(), address, sender);
        }
    }

    private void initCalculationStrategies(
            Set<CalculationStrategy> strategies, List<Consumer> nonPartitionedConsumers,
            Set<Address> nonPartitionedAddresses) {
        JobManager jobManager = taskContext.getJobContext().getJobManager();
        Map<Address, Address> hzToJetAddressMapping = jobManager.getJobContext().getHzToJetAddressMapping();
        List<Integer> localPartitions = JetUtil.getLocalPartitions(nodeEngine);
        for (Consumer consumer : shuffledConsumers) {
            MemberDistributionStrategy memberDistributionStrategy = consumer.getMemberDistributionStrategy();
            initConsumerCalculationStrategy(strategies, localPartitions, nonPartitionedConsumers, nonPartitionedAddresses,
                    hzToJetAddressMapping, consumer, memberDistributionStrategy);
        }
    }

    private void initConsumerCalculationStrategy(
            Set<CalculationStrategy> strategies, List<Integer> localPartitions,
            List<Consumer> nonPartitionedConsumers, Set<Address> nonPartitionedAddresses,
            Map<Address, Address> hzToJetAddressMapping, Consumer consumer,
            MemberDistributionStrategy memberDistributionStrategy) {
        if (memberDistributionStrategy == null) {
            initConsumerPartitions(strategies, localPartitions, nonPartitionedConsumers, nonPartitionedAddresses,
                    hzToJetAddressMapping, consumer);
        } else {
            Set<Member> members = new HashSet<>(memberDistributionStrategy.getTargetMembers(taskContext.getJobContext()));
            for (Member member : members) {
                Address address = member.getAddress();
                if (address.equals(nodeEngine.getThisAddress())) {
                    nonPartitionedConsumers.add(consumer);
                } else {
                    nonPartitionedAddresses.add(hzToJetAddressMapping.get(address));
                }
            }
        }
    }

    private void initConsumerPartitions(
            Set<CalculationStrategy> strategies, List<Integer> localPartitions,
            List<Consumer> nonPartitionedConsumers, Set<Address> nonPartitionedAddresses,
            Map<Address, Address> hzToJetAddressMapping, Consumer consumer) {
        CalculationStrategy calculationStrategy = new CalculationStrategy(
                consumer.getHashingStrategy(), consumer.getPartitionStrategy(), taskContext.getJobContext());
        final int partitionId = consumer.getPartitionId();
        if (!consumer.isPartitioned()) {
            if (partitionId < 0 || isPartitionLocal(nodeEngine, partitionId)) {
                nonPartitionedConsumers.add(consumer);
            } else {
                nonPartitionedAddresses.add(
                        hzToJetAddressMapping.get(nodeEngine.getPartitionService().getPartitionOwner(partitionId)));
            }
            return;
        }
        strategies.add(calculationStrategy);
        Map<Integer, List<Consumer>> map = partitionedConsumers.get(calculationStrategy);
        if (map == null) {
            map = new HashMap<>();
            partitionedConsumers.put(calculationStrategy, map);
        }
        if (partitionId >= 0) {
            processPartition(map, partitionId).add(consumer);
        } else {
            for (Integer localPartition : localPartitions) {
                processPartition(map, localPartition).add(consumer);
            }
        }
    }

    private void resetState() {
        lastConsumedSize = 0;
        chunkInProgress = false;
        localSuccess = isReceiver || !hasLocalConsumers;
    }

    private boolean processLocalConsumers(InputChunk<Object> chunk) throws Exception {
        boolean consumed = false;
        if (!isReceiver && hasLocalConsumers && !localSuccess) {
            localSuccess = super.onChunk(chunk);
            consumed = super.hasConsumed();
        }
        return consumed;
    }

    private boolean processChunkProgress() {
        boolean allFlushed = true;
        for (Consumer sender : senders) {
            allFlushed &= sender.isFlushed();
        }
        for (Consumer consumer : shuffledConsumers) {
            allFlushed &= consumer.isFlushed();
        }
        return allFlushed;
    }

    private int processShufflers(InputChunk<Object> chunk, int lastConsumedSize) throws Exception {
        if (calculationStrategies.length > 0) {
            int toIdx = Math.min(lastConsumedSize + chunkSize, chunk.size());
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
            if (!isReceiver) {
                sendToNonPartitionedRemotes(chunk);
            }
            flush();
            return chunk.size();
        }
    }

    private void flush() {
        for (Consumer consumer : shuffledConsumers) {
            consumer.flush();
        }
        if (!isReceiver) {
            for (Consumer sender : senders) {
                sender.flush();
            }
        }
    }

    private boolean processCalculationStrategy(
            Object object, CalculationStrategy calculationStrategy, int objectPartitionId,
            CalculationStrategy objectCalculationStrategy, boolean markedNonPartitionedRemotes
    ) throws Exception {
        Map<Integer, List<Consumer>> cache = partitionedConsumers.get(calculationStrategy);
        List<Consumer> writers = null;
        if (!cache.isEmpty()) {
            if (objectCalculationStrategy != null
                    && objectCalculationStrategy.equals(calculationStrategy)
                    && (objectPartitionId >= 0)
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
        Address address = nodeEngine.getPartitionService().getPartitionOwner(objectPartitionId);
        Address remoteJetAddress = taskContext.getJobContext().getHzToJetAddressMapping().get(address);
        Consumer sender = addrToSender.get(remoteJetAddress);
        if (sender != null) {
            markConsumer(sender);
            if (!markedNonPartitionedRemotes) {
                for (Address remoteAddress : nonPartitionedAddresses) {
                    if (!remoteAddress.equals(address)) {
                        markConsumer(addrToSender.get(remoteAddress));
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
                nodeEngine.getPartitionService().getPartitionCount());
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
        Arrays.fill(markers, null);
        boolean markedNonPartitionedRemotes = false;
        for (CalculationStrategy calculationStrategy : calculationStrategies) {
            markedNonPartitionedRemotes = processCalculationStrategy(
                    object, calculationStrategy, objectPartitionId, objectCalculationStrategy, markedNonPartitionedRemotes);
        }
        sendToMarked(object);
    }

    private void sendToMarked(Object object) throws Exception {
        for (Consumer marker : markers) {
            if (marker != null) {
                marker.consume(object);
            }
        }
    }

    private void markConsumer(Consumer consumer) {
        int position = markersCache.get(consumer);
        markers[position] = consumer;
    }

    private void writeToNonPartitionedLocals(InputChunk<Object> chunk) throws Exception {
        for (Consumer nonPartitionedWriter : nonPartitionedWriters) {
            nonPartitionedWriter.consume(chunk);
        }
    }

    private void sendToNonPartitionedRemotes(InputChunk<Object> chunk) throws Exception {
        for (Address remoteAddress : nonPartitionedAddresses) {
            addrToSender.get(remoteAddress).consume(chunk);
        }
    }

    private void writeToNonPartitionedLocals(Object object) throws Exception {
        for (Consumer nonPartitionedWriter : nonPartitionedWriters) {
            nonPartitionedWriter.consume(object);
        }
    }

    private void markNonPartitionedRemotes() throws Exception {
        for (Address remoteAddress : nonPartitionedAddresses) {
            markConsumer(addrToSender.get(remoteAddress));
        }
    }
}
