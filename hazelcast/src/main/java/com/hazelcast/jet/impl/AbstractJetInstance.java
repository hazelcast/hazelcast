/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.BasicJob;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.toList;

public abstract class AbstractJetInstance implements JetInstance {

    private final HazelcastInstance hazelcastInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;
    private final Map<String, Observable> observables;

    public AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(this));
        this.observables = new ConcurrentHashMap<>();
    }

    public long newJobId() {
        return jobRepository.get().newJobId();
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = newJobId();
        return newJob(jobId, dag, config);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        uploadResources(jobId, config);
        return newJobProxy(jobId, dag, config);
    }

    @Nonnull @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        long jobId = newJobId();
        return newJob(jobId, pipeline, config);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        config = config.attachAll(((PipelineImpl) pipeline).attachedFiles());
        uploadResources(jobId, config);
        return newJobProxy(jobId, pipeline, config);
    }

    private Job newJobInt(@Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        if (jobDefinition instanceof PipelineImpl) {
            return newJob((PipelineImpl) jobDefinition, config);
        } else {
            return newJob((DAG) jobDefinition, config);
        }
    }

    private Job newJobIfAbsent(@Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        if (config.getName() == null) {
            return newJobInt(jobDefinition, config);
        } else {
            while (true) {
                Job job = getJob(config.getName());
                if (job != null) {
                    JobStatus status = job.getStatus();
                    if (status != JobStatus.FAILED && status != JobStatus.COMPLETED) {
                        return job;
                    }
                }
                try {
                    return newJobInt(jobDefinition, config);
                } catch (JobAlreadyExistsException e) {
                    logFine(getLogger(), "Could not submit job with duplicate name: %s, ignoring", config.getName());
                }
            }
        }
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobIfAbsent((Object) dag, config);
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobIfAbsent((Object) pipeline, config);
    }

    @Nonnull @Override
    public BasicJob newLightJob(Pipeline pipeline) {
        return newLightJobInt(pipeline);
    }

    @Nonnull @Override
    public BasicJob newLightJob(DAG dag) {
        return newLightJobInt(dag);
    }

    @Nonnull
    public abstract BasicJob newLightJobInt(Object jobDefinition);

    @Override
    public BasicJob getJobById(long jobId) {
        try {
            return newJobProxy(jobId);
        } catch (Throwable t) {
            if (peel(t) instanceof JobNotFoundException) {
                return null;
            }
            throw rethrow(t);
        }
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return toList(getJobIdsByName(name), jobId -> (Job) newJobProxy(jobId));
    }

    @Nonnull @Override
    public Cluster getCluster() {
        return hazelcastInstance.getCluster();
    }

    @Nonnull @Override
    public String getName() {
        return hazelcastInstance.getName();
    }

    @Nonnull @Override
    public SqlService getSql() {
        return hazelcastInstance.getSql();
    }

    @Nonnull @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Nonnull @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return hazelcastInstance.getMap(name);
    }

    @Nonnull @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return hazelcastInstance.getReplicatedMap(name);
    }

    @Nonnull @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return hazelcastInstance.getList(name);
    }

    @Nonnull @Override
    public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
        return hazelcastInstance.getReliableTopic(name);
    }

    @Nonnull @Override
    public JetCacheManager getCacheManager() {
        return cacheManager;
    }

    @Nonnull @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        //noinspection unchecked
        return observables.computeIfAbsent(name, observableName ->
                new ObservableImpl<T>(observableName, hazelcastInstance, this::onDestroy, getLogger()));
    }

    @Nonnull
    @Override
    public Collection<Observable<?>> getObservables() {
        return hazelcastInstance.getDistributedObjects().stream()
                                .filter(o -> o.getServiceName().equals(RingbufferService.SERVICE_NAME))
                                .filter(o -> o.getName().startsWith(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX))
                                .map(o -> o.getName().substring(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX.length()))
                                .map(this::getObservable)
                                .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        hazelcastInstance.shutdown();
    }

    private void onDestroy(Observable<?> observable) {
        observables.remove(observable.name());
    }

    public abstract boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName);

    private long uploadResources(long jobId, JobConfig config) {
        return jobRepository.get().uploadJobResources(jobId, config);
    }

    public abstract ILogger getLogger();

    public abstract BasicJob newJobProxy(long jobId);

    public abstract Job newJobProxy(long jobId, Object jobDefinition, JobConfig config);

    public abstract List<Long> getJobIdsByName(String name);
}
