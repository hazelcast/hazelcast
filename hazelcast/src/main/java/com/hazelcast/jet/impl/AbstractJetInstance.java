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
import com.hazelcast.jet.BasicJob;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.stream.Collectors.toList;

public abstract class AbstractJetInstance<MemberIdType> implements JetInstance {

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
        return (Job) newJobInt(newJobId(), dag, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        return (Job) newJobInt(jobId, dag, config, false);
    }

    @Nonnull @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return (Job) newJobInt(newJobId(), pipeline, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return (Job) newJobInt(jobId, pipeline, config, false);
    }

    private BasicJob newJobInt(long jobId, @Nonnull Object jobDefinition, @Nullable JobConfig config, boolean isLightJob) {
        if (config != null) {
            if (jobDefinition instanceof PipelineImpl) {
                config = config.attachAll(((PipelineImpl) jobDefinition).attachedFiles());
            }
            uploadResources(jobId, config);
        }
        return newJobProxy(jobId, isLightJob, jobDefinition, config);
    }

    private Job newJobIfAbsent(@Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        if (config.getName() == null) {
            return (Job) newJobInt(newJobId(), jobDefinition, config, false);
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
                    return (Job) newJobInt(newJobId(), jobDefinition, config, false);
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
        return newJobInt(newJobId(), pipeline, null, true);
    }

    @Nonnull @Override
    public BasicJob newLightJob(DAG dag) {
        return newJobInt(newJobId(), dag, null, true);
    }

    @Nonnull @Override
    public List<BasicJob> getAllJobs() {
        Map<MemberIdType, GetJobIdsResult> results = getJobsInt(null);

        return results.entrySet().stream()
                .flatMap(en -> IntStream.range(0, en.getValue().getJobIds().length)
                        .mapToObj(i -> {
                            long jobId = en.getValue().getJobIds()[i];
                            boolean isLightJob = en.getValue().getIsLightJobs()[i];
                            return newJobProxy(jobId, isLightJob ? en.getKey() : null);
                        }))
                .collect(toList());
    }

    @Nonnull @Override
    public List<Job> getJobs(@NotNull String name) {
        // TODO [viliam]
        return null;
    }

    public abstract MemberIdType getMasterId();

    public abstract Map<MemberIdType, GetJobIdsResult> getJobsInt(Long onlyJobId);
    
    @Override
    public BasicJob getJobById(long jobId) {
        try {
            BasicJob job = null;
            Map<MemberIdType, GetJobIdsResult> jobs = getJobsInt(jobId);
            for (Entry<MemberIdType, GetJobIdsResult> resultEntry : jobs.entrySet()) {
                GetJobIdsResult result = resultEntry.getValue();
                assert result.getJobIds().length <= 1;
                if (result.getJobIds().length > 0) {
                    assert job == null : "duplicate job by ID";
                    boolean isLightJob = result.getIsLightJobs()[0];
                    job = newJobProxy(result.getJobIds()[0], isLightJob ? resultEntry.getKey() : null);
                }
            }
            return job;
        } catch (Throwable t) {
            throw rethrow(t);
        }
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

    private void uploadResources(long jobId, JobConfig config) {
        jobRepository.get().uploadJobResources(jobId, config);
    }

    public abstract ILogger getLogger();

    public abstract BasicJob newJobProxy(long jobId, MemberIdType coordinator);

    public abstract BasicJob newJobProxy(long jobId, boolean isLightJob, Object jobDefinition, JobConfig config);
}
