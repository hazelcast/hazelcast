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

package com.hazelcast.jet;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.impl.MapService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static java.util.stream.Collectors.toList;

/**
 * Represents either an instance of a Jet server node or a Jet client
 * instance that connects to a remote cluster.
 *
 * @since 3.0
 */
public interface JetInstance {

    /**
     * Returns the name of the Jet instance.
     */
    @Nonnull
    String getName();

    /**
     * Returns the underlying Hazelcast IMDG instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     */
    @Nonnull
    HazelcastInstance getHazelcastInstance();

    /**
     * Returns information about the cluster this Jet instance is part of.
     */
    @Nonnull
    Cluster getCluster();

    /**
     * Returns the configuration for this Jet member. This method is not
     * available on client instances.
     */
    @Nonnull
    JetConfig getConfig();

    /**
     * Creates and returns a Jet job based on the supplied DAG. Jet will
     * asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull DAG dag) {
        return newJob(dag, new JobConfig());
    }

    /**
     * Creates and returns an executable job based on the supplied pipeline.
     * Jet will asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull Pipeline pipeline) {
        return newJob(pipeline.toDag());
    }

    /**
     * Creates and returns a Jet job based on the supplied DAG and job
     * configuration. Jet will asynchronously start executing the job.
     *
     * <p>If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name, in which case it throws {@link
     * JobAlreadyExistsException}. Job is active if it is running,
     * suspended or waiting to be run; that is it has not completed or failed.
     * Thus there can be at most one active job with a given name at a time and
     * you can re-use the job name after the previous job completed.
     *
     * <p>See also {@link #newJobIfAbsent}.
     *
     * @throws JobAlreadyExistsException if there is an active job with
     *      an equal name
     */
    @Nonnull
    Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config);

    /**
     * Creates and returns a Jet job based on the supplied pipeline and job
     * configuration. Jet will asynchronously start executing the job.
     *
     * <p>If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name, in which case it throws {@link
     * JobAlreadyExistsException}. Job is active if it is running,
     * suspended or waiting to be run; that is it has not completed or failed.
     * Thus there can be at most one active job with a given name at a time and
     * you can re-use the job name after the previous job completed.
     *
     * <p>See also {@link #newJobIfAbsent}.
     *
     * @throws JobAlreadyExistsException if there is an active job with
     *      an equal name
     */
    @Nonnull
    default Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJob(pipeline.toDag(), config);
    }

    /**
     * Creates and returns a Jet job based on the supplied DAG and job
     * configuration. Jet will asynchronously start executing the job.
     *
     * <p>If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name. If there is, it will join that job instead
     * of submitting a new one. Job is active if it is running, suspended or
     * waiting to be run; that is it has not completed or failed. In other
     * words, this method ensures that the job with this name is running and is
     * not running multiple times in parallel.
     *
     * <p>This method is useful for microservices deployment when each package
     * contains a jet member and the job and you want the job to run only once.
     * But if the job is a batch job and runs very quickly, it can happen that
     * it executes multiple times, because the job name can be reused after a
     * previous execution completed.
     *
     * <p>If the job name is null, a new job is always submitted.
     *
     * <p>See also {@link #newJob}.
     */
    @Nonnull
    Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config);

    /**
     * Creates and returns a Jet job based on the supplied pipeline and job
     * configuration. Jet will asynchronously start executing the job.
     *
     * <p>If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name. If there is, it will join that job instead
     * of submitting a new one. Job is active if it is running, suspended or
     * waiting to be run; that is it has not completed or failed. In other
     * words, this method ensures that the job with this name is running and is
     * not running multiple times in parallel.
     *
     * <p>This method is useful for microservices deployment when each package
     * contains a jet member and the job and you want the job to run only once.
     * But if the job is a batch job and runs very quickly, it can happen that
     * it executes multiple times, because the job name can be reused after a
     * previous execution completed.
     *
     * <p>If the job name is null, a new job is always submitted.
     *
     * <p>See also {@link #newJob}.
     */
    @Nonnull
    default Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJobIfAbsent(pipeline.toDag(), config);
    }

    /**
     * Returns all submitted jobs including running and completed ones.
     */
    @Nonnull
    List<Job> getJobs();

    /**
     * Returns the job with the given id or {@code null} if no such job could
     * be found.
     */
    @Nullable
    Job getJob(long jobId);

    /**
     * Returns all jobs submitted with the given name, ordered in descending
     * order by submission time. The active job is always first. Empty list
     * will be returned if no job with the given name exists. The list includes
     * completed jobs.
     */
    @Nonnull
    List<Job> getJobs(@Nonnull String name);

    /**
     * Returns the active or last submitted job with the given name or {@code
     * null} if no such job could be found. The returned job can be already
     * completed.
     */
    @Nullable
    default Job getJob(@Nonnull String name) {
        return getJobs(name).stream().findFirst().orElse(null);
    }

    /**
     * Returns the {@link JobStateSnapshot} object representing an exported
     * snapshot with the given name. Returns {@code null} if no such snapshot
     * exists.
     */
    @Nullable
    default JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);
        if (!((AbstractJetInstance) this).existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
            return null;
        }
        IMapJet<Object, Object> map = getMap(mapName);
        Object validationRecord = map.get(SnapshotValidationRecord.KEY);
        if (validationRecord instanceof SnapshotValidationRecord) {
            // update the cache - for robustness. For example after the map was copied
            getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).set(name, validationRecord);
            return new JobStateSnapshot(this, name, (SnapshotValidationRecord) validationRecord);
        } else {
            return null;
        }
    }

    /**
     * Returns the collection of exported job state snapshots stored in the
     * cluster.
     */
    @Nonnull
    default Collection<JobStateSnapshot> getJobStateSnapshots() {
        return getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshot(this, (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(toList());
    }

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    @Nonnull
    <K, V> IMapJet<K, V> getMap(@Nonnull String name);

    /**
     * Returns the replicated map instance with the specified name.
     * <p>
     * A replicated map can be used for enriching a stream, see {@link
     * GeneralStage#mapUsingReplicatedMap(String, FunctionEx, BiFunctionEx)}.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     *
     */
    @Nonnull
    <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name);

    /**
     * Returns the distributed list instance with the specified name.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    @Nonnull
    <E> IListJet<E> getList(@Nonnull String name);

    /**
     * Obtain the {@link JetCacheManager} that provides access to JSR-107 (JCache) caches
     * configured on a Hazelcast Jet cluster.
     * <p>
     * Note that this method does not return a JCache {@code CacheManager}
     *
     * @return the Hazelcast Jet {@link JetCacheManager}
     * @see JetCacheManager
     */
    @Nonnull
    JetCacheManager getCacheManager();

    /**
     * Shuts down the current instance. If this is a client instance, it
     * disconnects the client. If this is a member instance, it gracefully
     * terminates the jobs running on it and, {@linkplain
     * JobConfig#setAutoScaling(boolean) if so configured}, restarts them after
     * this instance has shut down. When shutting down the entire cluster, it
     * is a good practice to manually {@linkplain Job#suspend suspend} all the
     * jobs so that they don't get restarted multiple times as each member
     * shuts down.
     * <p>
     * The call blocks until the instance is actually down.
     * <p>
     * <b>Note:</b> If you call {@code this.getHazelcastInstance().shutdown()},
     * it will cause all the jobs that run on this member to be forcefully
     * terminated, without creating a terminal snapshot. After the cluster
     * stabilizes again, Jet will restart them from the last snapshot that was
     * created some time ago.
     */
    void shutdown();
}
