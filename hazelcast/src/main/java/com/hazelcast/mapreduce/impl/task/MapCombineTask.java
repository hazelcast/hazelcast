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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.LifecycleMapper;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.MapReduceUtil;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.operation.KeysAssignmentOperation;
import com.hazelcast.mapreduce.impl.operation.KeysAssignmentResult;
import com.hazelcast.mapreduce.impl.operation.PostPonePartitionProcessing;
import com.hazelcast.mapreduce.impl.operation.RequestMemberIdAssignment;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionMapping;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessed;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionReducing;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.mapreduce.JobPartitionState.State.REDUCING;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.notifyRemoteException;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.CHECK_STATE_FAILED;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_MORE_PARTITIONS;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

/**
 * This class acutally executed the mapping-combine phase. It is responsible for opening / closing
 * the {@link com.hazelcast.mapreduce.KeyValueSource} implementation and possible configuring the
 * partitionId to operate on.
 *
 * @param <KeyIn>    type of the input key
 * @param <ValueIn>  type of the input value
 * @param <KeyOut>   type of the emitted key
 * @param <ValueOut> type of the emitted value
 * @param <Chunk>    type of the intermediate chunk (retrieved from combiners)
 */
public class MapCombineTask<KeyIn, ValueIn, KeyOut, ValueOut, Chunk> {

    private final AtomicBoolean cancelled = new AtomicBoolean();

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;
    private final MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> mappingPhase;
    private final KeyValueSource<KeyIn, ValueIn> keyValueSource;
    private final MapReduceService mapReduceService;
    private final IPartitionService partitionService;
    private final SerializationService serializationService;
    private final JobSupervisor supervisor;
    private final NodeEngine nodeEngine;
    private final String name;
    private final String jobId;
    private final int chunkSize;

    public MapCombineTask(JobTaskConfiguration configuration, JobSupervisor supervisor,
                          MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> mappingPhase) {

        this.mappingPhase = mappingPhase;
        this.supervisor = supervisor;
        this.mapper = configuration.getMapper();
        this.name = configuration.getName();
        this.jobId = configuration.getJobId();
        this.chunkSize = configuration.getChunkSize();
        this.nodeEngine = configuration.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.serializationService = nodeEngine.getSerializationService();
        this.mapReduceService = supervisor.getMapReduceService();
        this.keyValueSource = configuration.getKeyValueSource();
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void cancel() {
        cancelled.set(true);
        mappingPhase.cancel();
    }

    public void process() {
        ExecutorService es = mapReduceService.getExecutorService(name);
        if (keyValueSource instanceof PartitionIdAware) {
            es.submit(new PartitionBasedProcessor());
        } else {
            es.submit(new NonPartitionBasedProcessor());
        }
    }

    public final void processMapping(int partitionId, DefaultContext<KeyOut, ValueOut> context,
                                     KeyValueSource<KeyIn, ValueIn> keyValueSource, boolean partitionProcessor)
            throws Exception {

        context.setPartitionId(partitionId);
        context.setSerializationService((InternalSerializationService) serializationService);

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        int keyPreSelectorId = partitionProcessor ? partitionId : -1;
        if (mappingPhase.processingPartitionNecessary(keyPreSelectorId, partitionService)) {
            mappingPhase.executeMappingPhase(keyValueSource, mapper, context);
        }
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }
    }

    void onEmit(DefaultContext<KeyOut, ValueOut> context, int partitionId) {
        // If we have a reducer let's test for chunk size otherwise
        // we need to collect all values locally and wait for final request
        if (supervisor.getConfiguration().getReducerFactory() != null) {
            if (context.getCollected() == chunkSize) {
                Map<KeyOut, Chunk> chunkMap = context.requestChunk();

                // Wrap into IntermediateChunkNotification object
                Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(supervisor, chunkMap);

                // Register remote addresses and partitionId for receiving reducer events
                supervisor.registerReducerEventInterests(partitionId, mapping.keySet());

                for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
                    mapReduceService.sendNotification(entry.getKey(),
                            new IntermediateChunkNotification(entry.getKey(), name, jobId, entry.getValue(), partitionId));
                }
            }
        }
    }

    public static <K, V> Map<Address, Map<K, V>> mapResultToMember(JobSupervisor supervisor, Map<K, V> result) {

        Set<Object> unassignedKeys = new HashSet<Object>();
        for (Map.Entry<K, V> entry : result.entrySet()) {
            Address address = supervisor.getReducerAddressByKey(entry.getKey());
            if (address == null) {
                unassignedKeys.add(entry.getKey());
            }
        }

        if (unassignedKeys.size() > 0) {
            requestAssignment(unassignedKeys, supervisor);
        }

        // Now assign all keys
        Map<Address, Map<K, V>> mapping = new HashMap<Address, Map<K, V>>();
        for (Map.Entry<K, V> entry : result.entrySet()) {
            Address address = supervisor.getReducerAddressByKey(entry.getKey());
            if (address != null) {
                Map<K, V> data = mapping.get(address);
                if (data == null) {
                    data = new HashMap<K, V>();
                    mapping.put(address, data);
                }
                data.put(entry.getKey(), entry.getValue());
            }
        }
        return mapping;
    }

    private static void requestAssignment(Set<Object> keys, JobSupervisor supervisor) {
        try {
            MapReduceService mapReduceService = supervisor.getMapReduceService();
            String name = supervisor.getConfiguration().getName();
            String jobId = supervisor.getConfiguration().getJobId();
            KeysAssignmentResult assignmentResult = mapReduceService
                    .processRequest(supervisor.getJobOwner(), new KeysAssignmentOperation(name, jobId, keys));

            if (assignmentResult.getResultState() == SUCCESSFUL) {
                Map<Object, Address> assignment = assignmentResult.getAssignment();
                for (Map.Entry<Object, Address> entry : assignment.entrySet()) {
                    // Cache the keys for later mappings
                    if (!supervisor.assignKeyReducerAddress(entry.getKey(), entry.getValue())) {
                        throw new IllegalStateException("Key reducer assignment in illegal state");
                    }
                }
            }
        } catch (Exception e) {
            // Just announce it to higher levels
            throw new RuntimeException(e);
        }
    }


    private void finalizeMapping(int partitionId, DefaultContext<KeyOut, ValueOut> context)
            throws Exception {

        RequestPartitionResult result = mapReduceService
                .processRequest(supervisor.getJobOwner(), new RequestPartitionReducing(name, jobId, partitionId));

        if (result.getResultState() == SUCCESSFUL) {
            // If we have a reducer defined just send it over
            if (supervisor.getConfiguration().getReducerFactory() != null) {
                // Request a possible last chunk of data
                Map<KeyOut, Chunk> chunkMap = context.requestChunk();
                if (chunkMap.size() > 0) {
                    sendLastChunkToAssignedReducers(partitionId, chunkMap);

                } else {
                    finalizeProcessing(partitionId);
                }
            }
        }
    }

    private void finalizeProcessing(int partitionId)
            throws Exception {

        // If nothing to reduce we just set partition to processed
        RequestPartitionResult result = mapReduceService
                .processRequest(supervisor.getJobOwner(), new RequestPartitionProcessed(name, jobId, partitionId, REDUCING));

        if (result.getResultState() != SUCCESSFUL) {
            throw new RuntimeException("Could not finalize processing for partitionId " + partitionId);
        }
    }

    private void sendLastChunkToAssignedReducers(int partitionId, Map<KeyOut, Chunk> chunkMap) {
        Address sender = mapReduceService.getLocalAddress();

        // Wrap into LastChunkNotification object
        Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(supervisor, chunkMap);

        // Register remote addresses and partitionId for receiving reducer events
        supervisor.registerReducerEventInterests(partitionId, mapping.keySet());

        // Send LastChunk notifications
        for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
            Address receiver = entry.getKey();
            Map<KeyOut, Chunk> chunk = entry.getValue();
            mapReduceService
                    .sendNotification(receiver, new LastChunkNotification(receiver, name, jobId, sender, partitionId, chunk));
        }

        // Send LastChunk notification to notify reducers that received at least one chunk
        Set<Address> addresses = mapping.keySet();
        Collection<Address> reducerInterests = supervisor.getReducerEventInterests(partitionId);
        if (reducerInterests != null) {
            for (Address address : reducerInterests) {
                if (!addresses.contains(address)) {
                    mapReduceService.sendNotification(address,
                            new LastChunkNotification(address, name, jobId, sender, partitionId, Collections.emptyMap()));
                }
            }
        }
    }

    private void postponePartitionProcessing(int partitionId)
            throws Exception {

        RequestPartitionResult result = mapReduceService
                .processRequest(supervisor.getJobOwner(), new PostPonePartitionProcessing(name, jobId, partitionId));

        if (result.getResultState() != SUCCESSFUL) {
            throw new RuntimeException(
                    "Could not postpone processing for partitionId " + partitionId + " -> " + result.getResultState());
        }
    }

    private void handleProcessorThrowable(Throwable t) {
        notifyRemoteException(supervisor, t);
        if (t instanceof Error) {
            ExceptionUtil.sneakyThrow(t);
        }
    }

    private void processPartitionMapping(KeyValueSource<KeyIn, ValueIn> delegate, int partitionId, boolean partitionProcessor)
            throws Exception {
        delegate.reset();
        if (delegate.open(nodeEngine)) {
            DefaultContext<KeyOut, ValueOut> context = supervisor.getOrCreateContext(this);
            processMapping(partitionId, context, delegate, partitionProcessor);
            delegate.close();
            finalizeMapping(partitionId, context);
        } else {
            // Partition assignment might not be ready yet, postpone the processing and retry later
            postponePartitionProcessing(partitionId);
        }
    }

    /**
     * This class implements the partitionId based mapping phase
     */
    private class PartitionBasedProcessor
            implements Runnable {

        @Override
        public void run() {
            KeyValueSource<KeyIn, ValueIn> delegate = keyValueSource;
            if (supervisor.getConfiguration().isCommunicateStats()) {
                delegate = new KeyValueSourceFacade<KeyIn, ValueIn>(keyValueSource, supervisor);
            }

            try {
                // Force warmup of partition table!
                MapReduceUtil.enforcePartitionTableWarmup(mapReduceService);
            } catch (TimeoutException e) {
                handleProcessorThrowable(e);
            }

            processPartitions(delegate);
        }

        private void processPartitions(KeyValueSource<KeyIn, ValueIn> delegate) {
            while (true) {
                if (cancelled.get()) {
                    return;
                }

                Integer partitionId = findNewPartitionProcessing();
                if (partitionId == null) {
                    // Job's done
                    return;
                }

                // Migration event occurred, just retry
                if (partitionId == -1) {
                    continue;
                }

                try {
                    // This call cannot be delegated
                    ((PartitionIdAware) keyValueSource).setPartitionId(partitionId);
                    processPartitionMapping(delegate, partitionId, true);
                } catch (Throwable t) {
                    handleProcessorThrowable(t);
                }
            }
        }

        private Integer findNewPartitionProcessing() {
            try {
                RequestPartitionResult result = mapReduceService
                        .processRequest(supervisor.getJobOwner(), new RequestPartitionMapping(name, jobId));

                // JobSupervisor doesn't exists anymore on jobOwner, job done?
                if (result.getResultState() == NO_SUPERVISOR) {
                    return null;
                } else if (result.getResultState() == CHECK_STATE_FAILED) {
                    // retry
                    return -1;
                } else if (result.getResultState() == NO_MORE_PARTITIONS) {
                    return null;
                } else {
                    return result.getPartitionId();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * This class implements the non partitionId based mapping phase
     */
    private class NonPartitionBasedProcessor
            implements Runnable {

        @Override
        public void run() {
            try {
                // Force warmup of partition table!
                MapReduceUtil.enforcePartitionTableWarmup(mapReduceService);

                RequestPartitionResult result = mapReduceService
                        .processRequest(supervisor.getJobOwner(), new RequestMemberIdAssignment(name, jobId));

                // JobSupervisor doesn't exists anymore on jobOwner, job done?
                if (result.getResultState() == NO_SUPERVISOR) {
                    return;
                } else if (result.getResultState() == NO_MORE_PARTITIONS) {
                    return;
                }

                int partitionId = result.getPartitionId();

                KeyValueSource<KeyIn, ValueIn> delegate = keyValueSource;
                if (supervisor.getConfiguration().isCommunicateStats()) {
                    delegate = new KeyValueSourceFacade<KeyIn, ValueIn>(keyValueSource, supervisor);
                }

                processPartitionMapping(delegate, partitionId, false);
            } catch (Throwable t) {
                handleProcessorThrowable(t);
            }
        }
    }

}
