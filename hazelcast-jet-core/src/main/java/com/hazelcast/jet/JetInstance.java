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

package com.hazelcast.jet;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Represents either an instance of a Jet server node or a Jet client
 * instance that connects to a remote cluster.
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
     * Creates and returns a Jet job based on the supplied DAG and job
     * configuration. Jet will asynchronously start executing the job.
     */
    @Nonnull
    Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config);

    /**
     * Creates and returns an executable job based on the supplied pipeline.
     * Jet will asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull Pipeline pipeline) {
        return newJob(pipeline.toDag());
    }

    /**
     * Creates and returns a Jet job based on the supplied pipeline and job
     * configuration. Jet will asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
        return newJob(pipeline.toDag(), config);
    }

    /**
     * Returns all submitted jobs including running and completed ones.
     */
    @Nonnull
    List<Job> getJobs();

    /**
     * Returns the job with the given id or {@code null} if no such job could be found
     */
    @Nullable
    Job getJob(long jobId);

    /**
     * Returns all jobs submitted with the given name, ordered in descending order
     * by submission time. Empty list will be returned if no job with the given
     * name exists. The list includes completed jobs.
     */
    @Nonnull
    List<Job> getJobs(@Nonnull String name);

    /**
     * Returns the last submitted job with the given name or {@code null}
     * if no such job could be found.
     **/
    @Nullable
    default Job getJob(@Nonnull String name) {
        return getJobs(name).stream().findFirst().orElse(null);
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
     *
     * A replicated map can be used for enriching a stream, see
     * {@link GeneralStage#mapUsingReplicatedMap(String, DistributedBiFunction)}
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
     * Shuts down the current instance.
     */
    void shutdown();

}
