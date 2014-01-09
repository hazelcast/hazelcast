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

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

public class KeyValueJobOperation<K, V>
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private String name;
    private String jobId;
    private int chunkSize;
    private KeyValueSource<K, V> keyValueSource;
    private Mapper mapper;
    private CombinerFactory combinerFactory;
    private ReducerFactory reducerFactory;

    public KeyValueJobOperation() {
    }

    public KeyValueJobOperation(String name, String jobId, int chunkSize,
                                KeyValueSource<K, V> keyValueSource,
                                Mapper mapper, CombinerFactory combinerFactory,
                                ReducerFactory reducerFactory) {
        this.name = name;
        this.jobId = jobId;
        this.chunkSize = chunkSize;
        this.keyValueSource = keyValueSource;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        MapReduceService mapReduceService = getService();
        Address jobOwner = getCallerAddress();
        if (jobOwner == null) {
            jobOwner = getNodeEngine().getThisAddress();
        }
        JobSupervisor supervisor = mapReduceService.createJobSupervisor(
                new JobTaskConfiguration(jobOwner, getNodeEngine(), chunkSize, name,
                        jobId, mapper, combinerFactory, reducerFactory, keyValueSource));
        if (supervisor == null) {
            throw new IllegalStateException("Supervisor could not be created");
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeObject(keyValueSource);
        out.writeObject(mapper);
        out.writeObject(combinerFactory);
        out.writeObject(reducerFactory);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
        keyValueSource = in.readObject();
        mapper = in.readObject();
        combinerFactory = in.readObject();
        reducerFactory = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.TRACKED_JOB_OPERATION;
    }
}
