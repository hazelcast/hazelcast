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

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.LifecycleMapper;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessing;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.mapResultToMember;

public class MapCombineTask<KeyIn, ValueIn, KeyOut, ValueOut, Chunk> {

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;
    private final CombinerFactory<KeyOut, ValueOut, Chunk> combinerFactory;
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
        this.combinerFactory = configuration.getCombinerFactory();
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
        es.submit(new PartitionProcessor());
    }

    public final void processMapping() {
        DefaultContext<KeyOut, ValueOut> context = createContext();

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        mappingPhase.executeMappingPhase(keyValueSource, context);
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }

        Map<KeyOut, Chunk> chunkMap = context.finish();
        // Wrap into LastChunkNotification object
        Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);
        for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
            mapReduceService.sendNotification(entry.getKey(),
                    new LastChunkNotification(entry.getKey(), name, jobId, entry.getValue()));
        }
    }

    private DefaultContext<KeyOut, ValueOut> createContext() {
        return new DefaultContext<KeyOut, ValueOut>(combinerFactory, this);
    }

    void onEmit(DefaultContext<KeyOut, ValueOut> context) {
        if (context.getCollected() == chunkSize) {
            Map<KeyOut, Chunk> chunkMap = context.requestChunk();
            // Wrap into IntermediateChunkNotification object
            Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);
            for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
                mapReduceService.sendNotification(entry.getKey(),
                        new IntermediateChunkNotification(entry.getKey(), name, jobId, entry.getValue()));
            }
        }
    }

    private class PartitionProcessor implements Runnable {

        @Override
        public void run() {
            for (; ; ) {
                Integer partitionId = findNewPartitionProcessing();
                if (partitionId == null) {
                    // Job's done
                    return;
                }

                // Migration event occured, just retry
                if (partitionId == -1) {
                    continue;
                }

                try {
                    if (keyValueSource instanceof PartitionIdAware) {
                        ((PartitionIdAware) keyValueSource).setPartitionId(partitionId);
                    }

                    keyValueSource.reset();
                    keyValueSource.open(nodeEngine);
                    processMapping();
                    keyValueSource.close();
                } catch (IOException ignore) {
                    // TODO is ignoring the correct thing to do here?
                }
            }
        }

        private Integer findNewPartitionProcessing() {
            List<Integer> localPartitions = mapReduceService.getLocalPartitions();
            JobProcessInformationImpl processInformation = supervisor.getJobProcessInformation();
            JobPartitionState[] oldPartitionStates = processInformation.getPartitionStates();
            for (int i = 0; i < oldPartitionStates.length; i++) {
                try {
                    JobPartitionState partitionState = oldPartitionStates[i];
                    if (partitionState == null || partitionState.getState() == JobPartitionState.State.WAITING) {
                        if (localPartitions.contains(i)) {
                            JobPartitionState[] result = mapReduceService.processRequest(
                                    supervisor.getJobOwner(), new RequestPartitionProcessing(name, jobId, i));

                            // JobSupervisor doesn't exists anymore on jobOwner, job done?
                            if (result == null) {
                                return null;
                            } else {
                                return processNewPartitionStates(processInformation, result);
                            }
                        }
                    }
                } catch (Exception e) {
                    //TODO
                }
            }

            // No further partitions available?
            return null;
        }

        private int processNewPartitionStates(JobProcessInformationImpl processInformation,
                                              JobPartitionState[] newPartitionStates) {
            for (; ; ) {
                JobPartitionState[] oldPartitionStates = processInformation.getPartitionStates();
                if (processInformation.updatePartitionState(oldPartitionStates, newPartitionStates)) {
                    for (int i = 0; i < newPartitionStates.length; i++) {
                        JobPartitionState partitionState = newPartitionStates[i];
                        if (partitionState != null
                                && partitionState.getState() == JobPartitionState.State.PROCESSING
                                && partitionState.getOwner().equals(supervisor.getJobOwner())) {
                            return i;
                        }
                    }
                    return -1;
                }
            }
        }

    }

}
