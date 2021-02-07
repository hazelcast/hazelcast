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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.stream.Collectors.toList;

public abstract class AbstractJetInstance implements JetInstance {

    private final HazelcastInstance hazelcastInstance;
    private final Supplier<JobRepository> jobRepository;

    public AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(hazelcastInstance));
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = uploadResourcesAndAssignId(config);
        return newJobProxy(jobId, dag, config);
    }

    @Nonnull
    @Override
    public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        config = config.attachAll(((PipelineImpl) pipeline).attachedFiles());
        long jobId = uploadResourcesAndAssignId(config);
        return newJobProxy(jobId, pipeline, config);
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        return newJobIfAbsentInternal(dag, config);
    }

    @Nonnull
    @Override
    public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobIfAbsentInternal(pipeline, config);
    }

    private Job newJobIfAbsentInternal(Object jobDefinition, JobConfig jobConfig) {
        if (jobConfig.getName() == null) {
            return newJobInternal(jobDefinition, jobConfig);
        } else {
            while (true) {
                Job job = getJob(jobConfig.getName());
                if (job != null) {
                    JobStatus status = job.getStatus();
                    if (status != JobStatus.FAILED && status != JobStatus.COMPLETED) {
                        return job;
                    }
                }
                try {
                    return newJobInternal(jobDefinition, jobConfig);
                } catch (JobAlreadyExistsException e) {
                    logFine(getLogger(), "Could not submit job with duplicate name: %s, ignoring", jobConfig.getName());
                }
            }
        }
    }

    private Job newJobInternal(Object jobDefinition, JobConfig jobConfig) {
        if (jobDefinition instanceof DAG) {
            return newJob((DAG) jobDefinition, jobConfig);
        }
        return newJob((Pipeline) jobDefinition, jobConfig);
    }

    @Override
    public JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);

        if (!existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
            return null;
        }
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        Object validationRecord = map.get(SnapshotValidationRecord.KEY);
        if (validationRecord instanceof SnapshotValidationRecord) {
            // update the cache - for robustness. For example after the map was copied
            hazelcastInstance.getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).set(name, validationRecord);
            return new JobStateSnapshotImpl(hazelcastInstance, name, (SnapshotValidationRecord) validationRecord);
        } else {
            return null;
        }
    }

    @Override
    public Collection<JobStateSnapshot> getJobStateSnapshots() {
        return hazelcastInstance.getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshotImpl(hazelcastInstance, (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(toList());
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

    @Nonnull
    @Override
    public List<Job> getJobs(@Nonnull String name) {
        return Util.toList(getJobIdsByName(name), this::newJobProxy);
    }

    private long uploadResourcesAndAssignId(JobConfig config) {
        return jobRepository.get().uploadJobResources(config);
    }

    public abstract Job newJobProxy(long jobId);

    public abstract Job newJobProxy(long jobId, Object jobDefinition, JobConfig config);

    public abstract List<Long> getJobIdsByName(String name);

    public abstract boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName);

    public abstract ILogger getLogger();
}
