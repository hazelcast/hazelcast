/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.AccessControlException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.distinctBy;
import static com.hazelcast.security.permission.ActionConstants.ACTION_ADD_RESOURCES;
import static com.hazelcast.security.permission.ActionConstants.ACTION_SUBMIT;
import static java.util.stream.Collectors.toList;

/**
 * To not break the static factory methods of {@link Jet} that
 * return the deprecated {@link JetInstance}, we continue to
 * implement {@link JetInstance}, because we need to cast
 * instances of {@link AbstractJetInstance} to {@link JetInstance}
 * there. Search for casts to {@link JetInstance} before you consider
 * removing this.
 *
 * @param <M> the type of member ID (UUID or Address)
 */
@SuppressWarnings("deprecation") // we implement a deprecated API here
public abstract class AbstractJetInstance<M> implements JetInstance {

    // These permissions are configured via JobPermission
    // When the user doesn't configure the JobPermission correctly they get the permission violation on an internal
    // data structure. We translate these to proper JobPermission violations.
    private static final String FLAKE_ID_GENERATOR_JET_IDS_CREATE_DENIED_MESSAGE = "Permission " +
            "\\(\"com.hazelcast.security.permission.FlakeIdGeneratorPermission\" \"__jet.ids\" \"create\"\\) denied!";
    private static final String MAP_JET_RESOURCES_CREATE_DENIED_MESSAGE = "Permission " +
            "\\(\"com.hazelcast.security.permission.MapPermission\" \"__jet\\.resources\\..*\" \"create\"\\) denied!";

    private final HazelcastInstance hazelcastInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;
    private final Map<String, Observable> observables;

    public AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(hazelcastInstance));
        this.observables = new ConcurrentHashMap<>();
    }

    public long newJobId() {
        try {
            return jobRepository.get().newJobId();
        } catch (AccessControlException e) {
            if (e.getMessage().matches(FLAKE_ID_GENERATOR_JET_IDS_CREATE_DENIED_MESSAGE)) {
                AccessControlException ace = new AccessControlException("Permission " +
                        new JobPermission(ACTION_SUBMIT) + " denied!");
                ace.addSuppressed(e);
                throw ace;
            } else {
                throw e;
            }
        }
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), dag, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(jobId, dag, config, false);
    }

    @Nonnull @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), pipeline, config, false);
    }

    @Nonnull
    public Job newJob(long jobId, @Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(jobId, pipeline, config, false);
    }

    private Job newJobInt(long jobId, @Nonnull Object jobDefinition, @Nonnull JobConfig config, boolean isLightJob) {
        if (isLightJob) {
            validateConfigForLightJobs(config);
        }
        if (jobDefinition instanceof PipelineImpl) {
            config = config.attachAll(((PipelineImpl) jobDefinition).attachedFiles());
        }
        if (!config.getResourceConfigs().isEmpty()) {
            uploadResources(jobId, config);
        }
        return newJobProxy(jobId, isLightJob, jobDefinition, config);
    }

    protected static void validateConfigForLightJobs(JobConfig config) {
        Preconditions.checkTrue(config.getName() == null,
                "JobConfig.name not supported for light jobs");
        Preconditions.checkTrue(config.getResourceConfigs().isEmpty(),
                "Resources (jars, classes, attached files) not supported for light jobs");
        Preconditions.checkTrue(config.getProcessingGuarantee() == ProcessingGuarantee.NONE,
                "A processing guarantee not supported for light jobs");
        Preconditions.checkTrue(config.getClassLoaderFactory() == null,
                "JobConfig.classLoaderFactory not supported for light jobs");
        Preconditions.checkTrue(config.getInitialSnapshotName() == null,
                "JobConfig.initialSnapshotName not supported for light jobs");
    }

    private Job newJobIfAbsent(@Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        if (config.getName() == null) {
            return newJobInt(newJobId(), jobDefinition, config, false);
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
                    return newJobInt(newJobId(), jobDefinition, config, false);
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
    public Job newLightJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), pipeline, config, true);
    }

    @Nonnull @Override
    public Job newLightJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(newJobId(), dag, config, true);
    }

    @Nonnull
    public Job newLightJob(long jobId, @Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobInt(jobId, dag, config, true);
    }

    @Nonnull @Override
    public List<Job> getJobs() {
        return mergeJobIdsResults(getJobsInt(null, null));
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return mergeJobIdsResults(getJobsInt(name, null));
    }

    @Nullable @Override
    public Job getJob(long jobId) {
        List<Job> jobs = mergeJobIdsResults(getJobsInt(null, jobId));
        assert jobs.size() <= 1;
        return jobs.isEmpty() ? null : jobs.get(0);
    }

    @Nonnull
    private List<Job> mergeJobIdsResults(Map<M, GetJobIdsResult> results) {
        return results.entrySet().stream()
                .flatMap(en -> IntStream.range(0, en.getValue().getJobIds().length)
                        .mapToObj(i -> {
                            long jobId = en.getValue().getJobIds()[i];
                            boolean isLightJob = en.getValue().getIsLightJobs()[i];
                            return newJobProxy(jobId, isLightJob ? en.getKey() : null);
                        }))
                // In edge cases there can be duplicates. E.g. the GetIdsOp is broadcast to all members.  member1
                // is master and responds and dies. It's removed from cluster, member2 becomes master and
                // responds with the same normal jobs. It's safe to remove duplicates because the same jobId should
                // be the same job - we use FlakeIdGenerator to generate the IDs.
                .filter(distinctBy(Job::getId))
                .collect(toList());
    }

    @Nullable
    @Override
    public JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);
        if (!this.existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
            return null;
        }
        IMap<Object, Object> map = getHazelcastInstance().getMap(mapName);
        Object validationRecord = map.get(SnapshotValidationRecord.KEY);
        if (validationRecord instanceof SnapshotValidationRecord) {
            // update the cache - for robustness. For example after the map was copied
            getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).set(name, validationRecord);
            return new JobStateSnapshot(getHazelcastInstance(), name, (SnapshotValidationRecord) validationRecord);
        } else {
            return null;
        }
    }

    @Nonnull
    @Override
    public Collection<JobStateSnapshot> getJobStateSnapshots() {
        return getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshot(getHazelcastInstance(), (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(Collectors.toList());
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
        try {
            jobRepository.get().uploadJobResources(jobId, config);
        } catch (AccessControlException e) {
            if (e.getMessage().matches(MAP_JET_RESOURCES_CREATE_DENIED_MESSAGE)) {
                AccessControlException ace = new AccessControlException("Permission " +
                        new JobPermission(ACTION_ADD_RESOURCES) + " denied!");
                ace.addSuppressed(e);
                throw ace;
            } else {
                throw e;
            }
        }
    }

    public abstract ILogger getLogger();

    /**
     * Create a job proxy for a submitted job. {@code lightJobCoordinator} must
     * be non-null iff it's a light job.
     */
    public abstract Job newJobProxy(long jobId, M lightJobCoordinator);

    /**
     * Submit a new job and return the job proxy.
     */
    public abstract Job newJobProxy(long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config);

    public abstract Map<M, GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId);

    public abstract M getMasterId();
}
