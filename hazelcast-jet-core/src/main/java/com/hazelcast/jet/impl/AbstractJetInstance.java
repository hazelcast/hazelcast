/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.stream.Collectors.toList;

public abstract class AbstractJetInstance implements JetInstance {
    private final HazelcastInstance hazelcastInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;

    public AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(this));
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = uploadResourcesAndAssignId(config);
        return newJobProxy(jobId, dag, config);
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        if (config.getName() == null) {
            return newJob(dag, config);
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
                    return newJob(dag, config);
                } catch (JobAlreadyExistsException e) {
                    logFine(getLogger(), "Could not submit job with duplicate name: %s, ignoring", config.getName());
                }
            }
        }
    }

    @Override
    public Job getJob(long jobId) {
        try {
            Job job = newJobProxy(jobId);
            // get the status for the side-effect of throwing an exception if the jobId is invalid
            job.getStatus();
            return job;
        } catch (Throwable t) {
            if (peel(t) instanceof JobNotFoundException) {
                return null;
            }
            throw rethrow(t);
        }
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return getJobIdsByName(name).stream()
                                    .map(this::newJobProxy)
                                    .collect(toList());
    }

    @Nonnull @Override
    public Cluster getCluster() {
        return getHazelcastInstance().getCluster();
    }

    @Nonnull @Override
    public String getName() {
        return hazelcastInstance.getName();
    }

    @Nonnull @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Nonnull @Override
    public <K, V> IMapJet<K, V> getMap(@Nonnull String name) {
        return new IMapDecorator<>(hazelcastInstance.getMap(name), this);
    }

    @Nonnull @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return hazelcastInstance.getReplicatedMap(name);
    }

    @Nonnull @Override
    public <E> IListJet<E> getList(@Nonnull String name) {
        return new IListDecorator<>(hazelcastInstance.getList(name), this);
    }

    @Nonnull @Override
    public JetCacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void shutdown() {
        hazelcastInstance.shutdown();
    }

    public abstract boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName);

    private long uploadResourcesAndAssignId(JobConfig config) {
        return jobRepository.get().uploadJobResources(config);
    }

    public abstract ILogger getLogger();
    public abstract Job newJobProxy(long jobId);
    public abstract Job newJobProxy(long jobId, DAG dag, JobConfig config);
    public abstract List<Long> getJobIdsByName(String name);
}
