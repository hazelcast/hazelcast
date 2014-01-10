/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.LifecycleMapper;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.operation.ProcessStatsUpdateOperation;
import com.hazelcast.mapreduce.impl.operation.RequestMemberIdAssignment;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionMapping;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessed;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionReducing;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.mapreduce.JobPartitionState.State.REDUCING;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.mapResultToMember;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.CHECK_STATE_FAILED;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_MORE_PARTITIONS;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

public class MapCombineTask<KeyIn, ValueIn, KeyOut, ValueOut, Chunk> {

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;
    private final MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> mappingPhase;
    private final KeyValueSource<KeyIn, ValueIn> keyValueSource;
    private final MapReduceService mapReduceService;
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

    public void process() {
        ExecutorService es = mapReduceService.getExecutorService(name);
        if (keyValueSource instanceof PartitionIdAware) {
            es.submit(new PartitionProcessor());
        } else {
            es.submit(new SingleExecutionProcessor());
        }
    }

    public final void processMapping(int partitionId, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        DefaultContext<KeyOut, ValueOut> context = supervisor.createContext(this);
        context.setPartitionId(partitionId);

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        mappingPhase.executeMappingPhase(keyValueSource, mapper, context);
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }

        try {
            RequestPartitionResult result = mapReduceService.processRequest(supervisor.getJobOwner(),
                    new RequestPartitionReducing(name, jobId, partitionId), name);

            if (result.getResultState() == SUCCESSFUL) {
                // If we have a reducer defined just send it over
                if (supervisor.getConfiguration().getReducerFactory() != null) {
                    Map<KeyOut, Chunk> chunkMap = context.finish();
                    if (chunkMap.size() > 0) {
                        Address sender = mapReduceService.getLocalAddress();

                        // Wrap into LastChunkNotification object
                        Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);

                        // Register remote addresses and partitionId for receiving reducer events
                        supervisor.registerReducerEventInterests(partitionId, mapping.keySet());

                        // Send LastChunk notifications
                        for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
                            Address receiver = entry.getKey();
                            Map<KeyOut, Chunk> chunk = entry.getValue();
                            mapReduceService.sendNotification(receiver, new LastChunkNotification(
                                    receiver, name, jobId, sender, partitionId, chunk));
                        }

                        // Send LastChunk notification to notify reducers that received at least one chunk
                        Set<Address> addresses = mapping.keySet();
                        Collection<Address> reducerInterests = supervisor.getReducerEventInterests(partitionId);
                        if (reducerInterests != null) {
                            for (Address address : reducerInterests) {
                                if (!addresses.contains(address)) {
                                    mapReduceService.sendNotification(address, new LastChunkNotification(
                                            address, name, jobId, sender, partitionId, Collections.emptyMap()));
                                }
                            }
                        }

                    } else {
                        // If nothing to reduce we just set partition to processed
                        try {
                            result = mapReduceService.processRequest(supervisor.getJobOwner(),
                                    new RequestPartitionProcessed(name, jobId, partitionId, REDUCING), name);

                            if (result.getResultState() != SUCCESSFUL) {
                                throw new RuntimeException("Could not finalize processing for partitionId " + partitionId);
                            }
                        } catch (Exception ignore) {
                            ignore.printStackTrace();
                        }
                    }
                }
            } else {
                System.out.println(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void onEmit(DefaultContext<KeyOut, ValueOut> context, int partitionId) {
        // If we have a reducer let's test for chunk size otherwise
        // we need to collect all values locally and wait for final request
        if (supervisor.getConfiguration().getReducerFactory() != null) {
            if (context.getCollected() == chunkSize) {
                Map<KeyOut, Chunk> chunkMap = context.requestChunk();

                // Wrap into IntermediateChunkNotification object
                Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);

                // Register remote addresses and partitionId for receiving reducer events
                supervisor.registerReducerEventInterests(partitionId, mapping.keySet());

                for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
                    mapReduceService.sendNotification(entry.getKey(),
                            new IntermediateChunkNotification(entry.getKey(), name, jobId, entry.getValue(), partitionId));
                }
            }
        }
    }

    private class PartitionProcessor
            implements Runnable {

        @Override
        public void run() {
            KeyValueSource<KeyIn, ValueIn> delegate = keyValueSource;
            if (supervisor.getConfiguration().isCommunicateStats()) {
                delegate = new KeyValueSourceFacade<KeyIn, ValueIn>(keyValueSource, supervisor);
            }

            for (; ; ) {
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
                    delegate.reset();
                    delegate.open(nodeEngine);
                    processMapping(partitionId, delegate);
                    delegate.close();
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                }
            }
        }

        private Integer findNewPartitionProcessing() {
            try {
                RequestPartitionResult result = mapReduceService.processRequest(supervisor.getJobOwner(),
                        new RequestPartitionMapping(name, jobId), name);

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
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private class SingleExecutionProcessor
            implements Runnable {

        @Override
        public void run() {
            try {
                RequestPartitionResult result = mapReduceService.processRequest(supervisor.getJobOwner(),
                        new RequestMemberIdAssignment(name, jobId), name);

                // JobSupervisor doesn't exists anymore on jobOwner, job done?
                if (result.getResultState() == NO_SUPERVISOR) {
                    return;
                } else if (result.getResultState() == NO_MORE_PARTITIONS) {
                    return;
                }

                KeyValueSource<KeyIn, ValueIn> delegate = keyValueSource;
                if (supervisor.getConfiguration().isCommunicateStats()) {
                    delegate = new KeyValueSourceFacade<KeyIn, ValueIn>(keyValueSource, supervisor);
                }

                delegate.reset();
                delegate.open(nodeEngine);
                processMapping(result.getPartitionId(), delegate);
                delegate.close();
            } catch (Exception ignore) {
                ignore.printStackTrace();
            }
        }
    }

}
