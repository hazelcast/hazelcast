/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.CancellationException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.and;
import static com.hazelcast.cluster.memberselector.MemberSelectors.or;

/**
 * This operation is used to prepare a {@link com.hazelcast.mapreduce.KeyValueSource} based
 * map reduce operation on all cluster members.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class KeyValueJobOperation<K, V>
        extends Operation
        implements IdentifiedDataSerializable {

    /**
     * Selects members that will run this operation
     */
    public static final MemberSelector MEMBER_SELECTOR;

    static {
        // run this job on data members and the local member if it is a lite member
        MEMBER_SELECTOR = or(DATA_MEMBER_SELECTOR, and(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR));
    }

    private String name;
    private String jobId;
    private int chunkSize;
    private KeyValueSource<K, V> keyValueSource;
    private Mapper mapper;
    private CombinerFactory combinerFactory;
    private ReducerFactory reducerFactory;
    private boolean communicateStats;
    private TopologyChangedStrategy topologyChangedStrategy;

    public KeyValueJobOperation() {
    }

    public KeyValueJobOperation(String name, String jobId, int chunkSize, KeyValueSource<K, V> keyValueSource, Mapper mapper,
                                CombinerFactory combinerFactory, ReducerFactory reducerFactory, boolean communicateStats,
                                TopologyChangedStrategy topologyChangedStrategy) {
        this.name = name;
        this.jobId = jobId;
        this.chunkSize = chunkSize;
        this.keyValueSource = keyValueSource;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
        this.communicateStats = communicateStats;
        this.topologyChangedStrategy = topologyChangedStrategy;
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public void run()
            throws Exception {
        MapReduceService mapReduceService = getService();
        Address jobOwner = getCallerAddress();
        if (jobOwner == null) {
            jobOwner = getNodeEngine().getThisAddress();
        }

        // Inject managed context
        injectManagedContext(mapper, combinerFactory, reducerFactory, keyValueSource);

        // Build immutable configuration
        JobTaskConfiguration config = new JobTaskConfiguration(jobOwner, getNodeEngine(), chunkSize, name, jobId, mapper,
                combinerFactory, reducerFactory, keyValueSource, communicateStats, topologyChangedStrategy);

        JobSupervisor supervisor = mapReduceService.createJobSupervisor(config);

        if (supervisor == null) {
            // Supervisor was cancelled prior to creation
            AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.getJobTracker(name);
            TrackableJobFuture future = jobTracker.unregisterTrackableJob(jobId);
            if (future != null) {
                Exception exception = new CancellationException("Operation was cancelled by the user");
                future.setResult(exception);
            }
        }
    }

    private void injectManagedContext(Object injectee, Object... injectees) {
        ManagedContext managedContext = getNodeEngine().getSerializationService().getManagedContext();
        if (injectee != null) {
            managedContext.initialize(injectee);
        }
        for (Object otherInjectee : injectees) {
            if (otherInjectee != null) {
                managedContext.initialize(otherInjectee);
            }
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeObject(keyValueSource);
        out.writeObject(mapper);
        out.writeObject(combinerFactory);
        out.writeObject(reducerFactory);
        out.writeInt(chunkSize);
        out.writeBoolean(communicateStats);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
        keyValueSource = in.readObject();
        mapper = in.readObject();
        combinerFactory = in.readObject();
        reducerFactory = in.readObject();
        chunkSize = in.readInt();
        communicateStats = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.TRACKED_JOB_OPERATION;
    }


    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
