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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.KeyValueSourceMappingPhase;
import com.hazelcast.mapreduce.impl.task.MappingPhase;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CancellationException;

/**
 * This operation is used to start the actual processing after all node prepared to execute the map reduce job
 *
 * @param <K> type of the key
 */
public class StartProcessingJobOperation<K>
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private String name;
    private Collection<K> keys;
    private String jobId;
    private KeyPredicate<K> predicate;

    public StartProcessingJobOperation() {
    }

    public StartProcessingJobOperation(String name, String jobId, Collection<K> keys, KeyPredicate<K> predicate) {
        this.name = name;
        this.keys = keys;
        this.jobId = jobId;
        this.predicate = predicate;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public void run()
            throws Exception {
        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);
        if (supervisor == null) {
            if (mapReduceService.unregisterJobSupervisorCancellation(name, jobId)) {
                // Supervisor was cancelled prior to creation
                AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.getJobTracker(name);
                TrackableJobFuture future = jobTracker.unregisterTrackableJob(jobId);
                if (future != null) {
                    Exception exception = new CancellationException("Operation was cancelled by the user");
                    future.setResult(exception);
                }
            }
            return;
        }

        // Create actual mapping operation
        MappingPhase mappingPhase = new KeyValueSourceMappingPhase(keys, predicate);
        supervisor.startTasks(mappingPhase);
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeInt(keys == null ? 0 : keys.size());
        if (keys != null) {
            for (Object key : keys) {
                out.writeObject(key);
            }
        }
        out.writeObject(predicate);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
        int size = in.readInt();
        keys = new ArrayList<K>();
        for (int i = 0; i < size; i++) {
            keys.add((K) in.readObject());
        }
        predicate = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.START_PROCESSING_OPERATION;
    }
}
