/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapReduceCancelCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForCustomCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForListCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForMapCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForMultiMapCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceForSetCodec;
import com.hazelcast.client.impl.protocol.codec.MapReduceJobProcessInformationCodec;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.TrackableJob;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.ListKeyValueSource;
import com.hazelcast.mapreduce.impl.MapKeyValueSource;
import com.hazelcast.mapreduce.impl.MultiMapKeyValueSource;
import com.hazelcast.mapreduce.impl.SetKeyValueSource;
import com.hazelcast.mapreduce.impl.task.TransferableJobProcessInformation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.UuidUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class ClientMapReduceProxy
        extends ClientProxy
        implements JobTracker {

    private final ConcurrentMap<String, ClientTrackableJob> trackableJobs = new ConcurrentHashMap<String, ClientTrackableJob>();

    public ClientMapReduceProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {
        for (ClientTrackableJob trackableJob : trackableJobs.values()) {
            trackableJob.completableFuture.cancel(false);
        }
    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new ClientJob<K, V>(name, source);
    }

    @Override
    public <V> TrackableJob<V> getTrackableJob(String jobId) {
        return trackableJobs.get(jobId);
    }

    @Override
    public String toString() {
        return "JobTracker{" + "name='" + name + '\'' + '}';
    }

    private ClientMessage invoke(ClientMessage request, String jobId) throws Exception {
        ClientTrackableJob trackableJob = trackableJobs.get(jobId);
        if (trackableJob != null) {
            ClientConnection sendConnection = trackableJob.clientInvocation.getSendConnectionOrWait();
            Address runningMember = sendConnection.getEndPoint();
            final ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, runningMember);
            ClientInvocationFuture future = clientInvocation.invoke();
            return future.get();
        }
        return null;
    }

    private class ClientJob<KeyIn, ValueIn>
            extends AbstractJob<KeyIn, ValueIn> {

        public ClientJob(String name, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
            super(name, ClientMapReduceProxy.this, keyValueSource);
        }

        @Override
        protected <T> JobCompletableFuture<T> invoke(final Collator collator) {
            try {
                final String jobId = UuidUtil.newUnsecureUuidString();

                ClientMessage request = getRequest(name, jobId, keys, predicate, mapper, combinerFactory, reducerFactory,
                        keyValueSource, chunkSize, topologyChangedStrategy);

                final ClientCompletableFuture completableFuture = new ClientCompletableFuture(jobId);

                final ClientInvocation clientInvocation = new ClientInvocation(getClient(), request);
                final ClientInvocationFuture future = clientInvocation.invoke();

                future.andThen(new ExecutionCallback() {
                    @Override
                    public void onResponse(Object res) {
                        Map map = toObjectMap((ClientMessage) res);

                        Object response = map;
                        try {
                            if (collator != null) {
                                response = collator.collate(((Map) response).entrySet());
                            }
                        } finally {
                            completableFuture.setResult(response);
                            trackableJobs.remove(jobId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        Throwable t = throwable;
                        try {
                            if (t instanceof ExecutionException && t.getCause() instanceof CancellationException) {
                                t = t.getCause();
                            }
                            completableFuture.setResult(t);
                        } finally {
                            trackableJobs.remove(jobId);
                        }
                    }
                });

                trackableJobs.putIfAbsent(jobId, new ClientTrackableJob<T>(jobId, clientInvocation, completableFuture));
                return completableFuture;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private Map toObjectMap(ClientMessage res) {
        SerializationService serializationService = getContext().getSerializationService();
        Collection<Map.Entry<Data, Data>> entries = MapReduceForCustomCodec.decodeResponse(res).response;
        HashMap hashMap = new HashMap();
        for (Map.Entry<Data, Data> entry : entries) {
            Object key = serializationService.toObject(entry.getKey());
            Object value = serializationService.toObject(entry.getValue());
            hashMap.put(key, value);
        }
        return hashMap;
    }

    private ClientMessage getRequest(String name, String jobId, Collection keys, KeyPredicate predicate, Mapper mapper,
                                     CombinerFactory combinerFactory, ReducerFactory reducerFactory,
                                     KeyValueSource keyValueSource, int chunkSize,
                                     TopologyChangedStrategy topologyChangedStrategy) {
        Data predicateData = toData(predicate);
        Data mapperData = toData(mapper);
        Data combinerFactoryData = toData(combinerFactory);
        Data reducerFactoryData = toData(reducerFactory);
        List<Data> list = null;
        if (keys != null) {
            list = new ArrayList<Data>(keys.size());
            for (Object key : keys) {
                list.add(toData(key));
            }
        }

        String topologyChangedStrategyName = null;
        if (topologyChangedStrategy != null) {
            topologyChangedStrategyName = topologyChangedStrategy.name();
        }

        if (keyValueSource instanceof MapKeyValueSource) {
            MapKeyValueSource source = (MapKeyValueSource) keyValueSource;
            return MapReduceForMapCodec
                    .encodeRequest(name, jobId, predicateData, mapperData, combinerFactoryData, reducerFactoryData,
                            source.getMapName(), chunkSize, list, topologyChangedStrategyName);
        } else if (keyValueSource instanceof ListKeyValueSource) {
            ListKeyValueSource source = (ListKeyValueSource) keyValueSource;
            return MapReduceForListCodec
                    .encodeRequest(name, jobId, predicateData, mapperData, combinerFactoryData, reducerFactoryData,
                            source.getListName(), chunkSize, list, topologyChangedStrategyName);
        } else if (keyValueSource instanceof SetKeyValueSource) {
            SetKeyValueSource source = (SetKeyValueSource) keyValueSource;
            return MapReduceForSetCodec
                    .encodeRequest(name, jobId, predicateData, mapperData, combinerFactoryData, reducerFactoryData,
                            source.getSetName(), chunkSize, list, topologyChangedStrategyName);
        } else if (keyValueSource instanceof MultiMapKeyValueSource) {
            MultiMapKeyValueSource source = (MultiMapKeyValueSource) keyValueSource;
            return MapReduceForMultiMapCodec
                    .encodeRequest(name, jobId, predicateData, mapperData, combinerFactoryData, reducerFactoryData,
                            source.getMultiMapName(), chunkSize, list, topologyChangedStrategyName);
        }
        return MapReduceForCustomCodec
                .encodeRequest(name, jobId, predicateData, mapperData, combinerFactoryData, reducerFactoryData,
                        toData(keyValueSource), chunkSize, list, topologyChangedStrategyName);

    }

    private class ClientCompletableFuture<V>
            extends AbstractCompletableFuture<V>
            implements JobCompletableFuture<V> {

        private final String jobId;

        protected ClientCompletableFuture(String jobId) {
            super(getContext().getExecutionService().getAsyncExecutor(), Logger.getLogger(ClientCompletableFuture.class));
            this.jobId = jobId;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        protected boolean shouldCancel(boolean mayInterruptIfRunning) {
            boolean cancelled = false;
            try {
                ClientMessage request = MapReduceCancelCodec.encodeRequest(name, jobId);
                ClientMessage response = invoke(request, jobId);
                cancelled = MapReduceCancelCodec.decodeResponse(response).response;
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
            return cancelled;
        }

        @Override
        protected void setResult(Object result) {
            super.setResult(result);
        }

    }

    private final class ClientTrackableJob<V>
            implements TrackableJob<V> {

        private final String jobId;
        private final ClientInvocation clientInvocation;
        private final AbstractCompletableFuture<V> completableFuture;

        private ClientTrackableJob(String jobId, ClientInvocation clientInvocation,
                                   AbstractCompletableFuture<V> completableFuture) {
            this.jobId = jobId;
            this.clientInvocation = clientInvocation;
            this.completableFuture = completableFuture;
        }

        @Override
        public JobTracker getJobTracker() {
            return ClientMapReduceProxy.this;
        }

        @Override
        public String getName() {
            return ClientMapReduceProxy.this.name;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public ICompletableFuture<V> getCompletableFuture() {
            return completableFuture;
        }

        @Override
        public JobProcessInformation getJobProcessInformation() {
            try {
                ClientMessage request = MapReduceJobProcessInformationCodec.encodeRequest(name, jobId);

                MapReduceJobProcessInformationCodec.ResponseParameters responseParameters = MapReduceJobProcessInformationCodec
                        .decodeResponse(invoke(request, jobId));
                JobPartitionState[] partitionStates = responseParameters.jobPartitionStates.toArray(new JobPartitionState[0]);
                return new TransferableJobProcessInformation(partitionStates, responseParameters.processRecords);
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
            }
            return null;
        }

    }

}
