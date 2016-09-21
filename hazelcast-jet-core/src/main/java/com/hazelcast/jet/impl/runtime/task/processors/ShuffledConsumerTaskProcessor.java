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
import com.hazelcast.jet.WithPartitionId;
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
import com.hazelcast.jet.strategy.WithCalculationStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.HashUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.impl.util.JetUtil.isPartitionLocal;
import static java.util.Collections.singletonList;


public class ShuffledConsumerTaskProcessor extends ConsumerTaskProcessor {
    private final boolean isSender;
    private final boolean hasLocalConsumers;
    private final Consumer[] senders;
    private final Consumer[] shuffledConsumers;
    private Address[] nonPartitionedAddresses;
    private Consumer[] nonPartitionedConsumers;
    private final Consumer[] markers;
    private final Map<Consumer, Integer> consumerToMarkerIdx = new HashMap<>();
    private CalculationStrategy[] calculationStrategies;
    private final Map<Address, Consumer> addrToSender = new HashMap<>();
    private final Map<CalculationStrategy, Map<Integer, List<Consumer>>> partitionedConsumers = new HashMap<>();
    private final NodeEngine nodeEngine;

    private boolean consumptionDone;
    private boolean flushingInProgress;
    private boolean localSuccess;

    public ShuffledConsumerTaskProcessor(
            Consumer[] consumers, Processor processor, TaskContext taskContext, boolean isSender) {
        super(filterConsumers(consumers, false), processor, taskContext);
        this.nodeEngine = taskContext.getJobContext().getNodeEngine();
        this.isSender = isSender;
        this.hasLocalConsumers = this.consumers.length > 0;
        this.shuffledConsumers = filterConsumers(consumers, true);
        if (isSender) {
            initSenders();
            this.senders = addrToSender.values().toArray(new Consumer[addrToSender.size()]);
            this.markers = new Consumer[senders.length + shuffledConsumers.length];
        } else {
            this.senders = new Consumer[0];
            this.markers = new Consumer[shuffledConsumers.length];
        }
        initMarkers();
        initCalculationStrategies();
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
            consumptionDone = true;
            consumedSome = false;
            return true;
        }
        boolean success = false;
        boolean consumedSome = feedLocalConsumers(inputChunk);
        if (!flushingInProgress && !consumptionDone) {
            consumeSome(inputChunk);
            consumedSome = true;
            consumptionDone = true;
            flushingInProgress = true;
        }
        if (flushingInProgress) {
            if (flushSomeAndSeeIfDone()) {
                flushingInProgress = false;
            }
            consumedSome = true;
        }
        if (!flushingInProgress && consumptionDone && localSuccess) {
            success = true;
            consumedSome = true;
        }
        if (success) {
            resetState();
        }
        this.consumedSome = consumedSome;
        return success;
    }

    private static Consumer[] filterConsumers(Consumer[] consumers, boolean isShuffled) {
        return Arrays.stream(consumers)
                     .filter(c -> c.isShuffled() == isShuffled)
                     .toArray(Consumer[]::new);
    }


    private void initMarkers() {
        int position = 0;
        for (Consumer sender : senders) {
            consumerToMarkerIdx.put(sender, position++);
        }
        for (Consumer shuffledConsumer : shuffledConsumers) {
            consumerToMarkerIdx.put(shuffledConsumer, position++);
        }
    }

    private void initSenders() {
        final JobManager jobManager = taskContext.getJobContext().getJobManager();
        final VertexRunner vertexRunner = jobManager.getRunnerByVertex(taskContext.getVertex());
        final int taskNumber = taskContext.getTaskNumber();
        final VertexTask vertexTask = vertexRunner.getTask(taskNumber);
        for (Address address : jobManager.getJobContext().getSocketWriters().keySet()) {
            ShufflingSender sender = new ShufflingSender(vertexTask, vertexRunner.getId());
            addrToSender.put(address, sender);
            jobManager.registerShufflingSender(taskNumber, vertexRunner.getId(), address, sender);
        }
    }

    private void initCalculationStrategies() {
        final Set<CalculationStrategy> strategies = new HashSet<>();
        final List<Consumer> nonPartitionedConsumers = new ArrayList<>(this.shuffledConsumers.length);
        final Set<Address> nonPartitionedAddresses = new HashSet<>(this.shuffledConsumers.length);
        final List<Integer> localPartitions = JetUtil.getLocalPartitions(nodeEngine);
        for (Consumer consumer : shuffledConsumers) {
            final int partitionId = consumer.getPartitionId();
            final MemberDistributionStrategy distStrat = consumer.getMemberDistributionStrategy();
            if (distStrat != null) {
                final Set<Member> members = distStrat.getTargetMembers(taskContext.getJobContext());
                for (Member member : members) {
                    final Address address = member.getAddress();
                    if (address.equals(nodeEngine.getThisAddress())) {
                        nonPartitionedConsumers.add(consumer);
                    } else {
                        nonPartitionedAddresses.add(toJetAddress(address));
                    }
                }
            } else if (consumer.isPartitioned()) {
                final CalculationStrategy calculationStrategy = new CalculationStrategy(
                        consumer.getHashingStrategy(), consumer.getPartitionStrategy(), taskContext.getJobContext());
                strategies.add(calculationStrategy);
                final Map<Integer, List<Consumer>> map =
                        partitionedConsumers.computeIfAbsent(calculationStrategy, x -> new HashMap<>());
                final List<Integer> ptionIds = partitionId >= 0 ? singletonList(partitionId) : localPartitions;
                for (Integer id : ptionIds) {
                    map.computeIfAbsent(id, ArrayList::new).add(consumer);
                }
            } else if (partitionId < 0 || isPartitionLocal(nodeEngine, partitionId)) {
                nonPartitionedConsumers.add(consumer);
            } else {
                nonPartitionedAddresses.add(
                        toJetAddress(nodeEngine.getPartitionService().getPartitionOwner(partitionId)));
            }
        }
        this.calculationStrategies = strategies.toArray(new CalculationStrategy[strategies.size()]);
        this.nonPartitionedConsumers = nonPartitionedConsumers.toArray(new Consumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
    }

    private Address toJetAddress(Address hzAddress) {
        return taskContext.getJobContext().getJobManager().getJobContext().toJetAddress(hzAddress);
    }

    private void resetState() {
        consumptionDone = false;
        flushingInProgress = false;
        localSuccess = !isSender || !hasLocalConsumers;
    }

    private boolean flushSomeAndSeeIfDone() {
        boolean allFlushed = true;
        for (Consumer sender : senders) {
            allFlushed &= sender.isFlushed();
        }
        for (Consumer consumer : shuffledConsumers) {
            allFlushed &= consumer.isFlushed();
        }
        return allFlushed;
    }

    private void flush() {
        for (Consumer consumer : shuffledConsumers) {
            consumer.flush();
        }
        if (isSender) {
            for (Consumer sender : senders) {
                sender.flush();
            }
        }
    }

    private void consume(Object object) throws Exception {
        final CalculationStrategy objectCalculationStrategy = object instanceof WithCalculationStrategy
                ? ((WithCalculationStrategy) object).getCalculationStrategy() : null;
        final int objectPartitionId = object instanceof WithPartitionId
                ? ((WithPartitionId) object).getPartitionId() : -1;
        feedNonPartitionedConsumers(object);
        Arrays.fill(markers, null);
        boolean didMarkNonPartitionedRemotes = false;
        for (CalculationStrategy calculationStrategy : calculationStrategies) {
            didMarkNonPartitionedRemotes = consume(
                    object, calculationStrategy, objectPartitionId, objectCalculationStrategy, didMarkNonPartitionedRemotes);
        }
        sendToMarked(object);
    }

    private void consumeSome(InputChunk<Object> chunk) throws Exception {
        if (calculationStrategies.length > 0) {
            for (int i = 0; i < chunk.size(); i++) {
                Object object = chunk.get(i);
                consume(object);
            }
        } else {
            feedNonPartitionedConsumers(chunk);
            if (isSender) {
                sendToNonPartitionedRemotes(chunk);
            }
        }
        flush();
    }

    private boolean consume(
            Object object, CalculationStrategy calculationStrategy, int objectPartitionId,
            CalculationStrategy objectCalculationStrategy, boolean didMarkNonPartitionedRemotes
    ) throws Exception {
        final Map<Integer, List<Consumer>> cache = partitionedConsumers.get(calculationStrategy);
        List<Consumer> consumers = null;
        if (cache.isEmpty()) {
            //Send to another node
            objectPartitionId = calculatePartitionId(object, calculationStrategy);
        } else if (objectCalculationStrategy != null
                && objectCalculationStrategy.equals(calculationStrategy)
                && objectPartitionId >= 0
        ) {
            consumers = cache.get(objectPartitionId);
        } else {
            objectPartitionId = calculatePartitionId(object, calculationStrategy);
            consumers = cache.get(objectPartitionId);
        }
        Address address = nodeEngine.getPartitionService().getPartitionOwner(objectPartitionId);
        Address remoteJetAddress = taskContext.getJobContext().toJetAddress(address);
        Consumer sender = addrToSender.get(remoteJetAddress);
        if (sender != null) {
            markConsumer(sender);
            if (!didMarkNonPartitionedRemotes) {
                for (Address remoteAddress : nonPartitionedAddresses) {
                    if (!remoteAddress.equals(address)) {
                        markConsumer(addrToSender.get(remoteAddress));
                    }
                }
            }
        } else if (!JetUtil.isEmpty(consumers)) {
            for (Consumer consumer : consumers) {
                markConsumer(consumer);
            }
            if (!didMarkNonPartitionedRemotes) {
                markNonPartitionedRemotes();
                didMarkNonPartitionedRemotes = true;
            }
        }
        return didMarkNonPartitionedRemotes;
    }

    private int calculatePartitionId(Object object, CalculationStrategy calculationStrategy) {
        return HashUtil.hashToIndex(calculationStrategy.hash(object),
                nodeEngine.getPartitionService().getPartitionCount());
    }

    private void sendToMarked(Object object) throws Exception {
        for (Consumer marker : markers) {
            if (marker != null) {
                marker.consume(object);
            }
        }
    }

    private void markConsumer(Consumer consumer) {
        markers[consumerToMarkerIdx.get(consumer)] = consumer;
    }

    private boolean feedLocalConsumers(InputChunk<Object> chunk) throws Exception {
        if (isSender && hasLocalConsumers && !localSuccess) {
            localSuccess = super.onChunk(chunk);
            return super.didWork();
        }
        return false;
    }

    private void feedNonPartitionedConsumers(InputChunk<Object> chunk) throws Exception {
        for (Consumer c : nonPartitionedConsumers) {
            c.consume(chunk);
        }
    }

    private void feedNonPartitionedConsumers(Object object) throws Exception {
        for (Consumer nonPartitionedWriter : nonPartitionedConsumers) {
            nonPartitionedWriter.consume(object);
        }
    }

    private void sendToNonPartitionedRemotes(InputChunk<Object> chunk) throws Exception {
        for (Address remoteAddress : nonPartitionedAddresses) {
            addrToSender.get(remoteAddress).consume(chunk);
        }
    }

    private void markNonPartitionedRemotes() throws Exception {
        for (Address remoteAddress : nonPartitionedAddresses) {
            markConsumer(addrToSender.get(remoteAddress));
        }
    }
}
