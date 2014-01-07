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

import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.TrackableJob;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.mapreduce.impl.task.MapCombineTask;
import com.hazelcast.mapreduce.impl.task.MappingPhase;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import sun.net.www.content.text.plain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyValueJobOperation<K, V>
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private String name;
    private List<K> keys;
    private String jobId;
    private int chunkSize;
    private KeyPredicate<K> predicate;
    private KeyValueSource<K, V> keyValueSource;
    private Mapper mapper;
    private CombinerFactory combinerFactory;
    private ReducerFactory reducerFactory;

    public KeyValueJobOperation() {
    }

    public KeyValueJobOperation(String name, String jobId, int chunkSize, List<K> keys,
                                KeyPredicate<K> predicate, KeyValueSource<K, V> keyValueSource,
                                Mapper mapper, CombinerFactory combinerFactory,
                                ReducerFactory reducerFactory) {
        this.name = name;
        this.keys = keys;
        this.jobId = jobId;
        this.chunkSize = chunkSize;
        this.keyValueSource = keyValueSource;
        this.mapper = mapper;
        this.predicate = predicate;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
    }

    @Override
    public void run() throws Exception {
        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.createJobSupervisor(
                new JobTaskConfiguration(chunkSize, name, jobId, mapper,
                        combinerFactory, reducerFactory, keyValueSource));

        MappingPhase mappingPhase = new KeyValueSourceMappingPhase(mapper, keys, predicate);
        supervisor.startTasks(mappingPhase);
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeObject(keyValueSource);
        out.writeInt(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            out.writeObject(keys);
        }
        out.writeObject(mapper);
        out.writeObject(combinerFactory);
        out.writeObject(reducerFactory);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
        keyValueSource = in.readObject();
        int size = in.readInt();
        keys = new ArrayList<K>();
        for (int i = 0; i < size; i++) {
            keys.add((K) in.readObject());
        }
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
