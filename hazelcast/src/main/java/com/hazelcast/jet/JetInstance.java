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

package com.hazelcast.jet;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static java.util.stream.Collectors.toList;


/**
 * @since Jet 3.0
 *
 * @deprecated After 5.0 merge of Hazelcast products (IMDG and Jet into
 * single Hazelcast product), we represent Jet as an extension service
 * to a Hazelcast product instead of being an instance on its own which
 * encapsulates {@link HazelcastInstance}. Please use {@link JetService}
 * instead.
 */
@Deprecated
public interface JetInstance extends JetService {

    /**
     * Returns the underlying Hazelcast instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     * @since Jet 3.0
     * @deprecated since 5.0
     * Because we first access to {@link HazelcastInstance} and then
     * {@link JetService} from the product's entry point -{@link Hazelcast}-,
     * we don't need to this back reference anymore. This class made
     * sense when the entry point was {@link Jet}.
     */
    @Nonnull
    @Deprecated
    HazelcastInstance getHazelcastInstance();

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getName()} instead.
     */
    @Nonnull
    @Deprecated
    String getName();

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getCluster()} instead.
     */
    @Nonnull
    @Deprecated
    Cluster getCluster();

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * We left it here since it has a default implementation depending
     * on {@link JetInstance#getHazelcastInstance()}.
     * Prefer to use {@link JetService#getJobStateSnapshot(String)}
     * instead.
     */
    @Nullable
    @Override
    @Deprecated
    default JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);
        if (!((AbstractJetInstance) this).existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
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

    /**
     * @since Jet 3.0
     * @deprecated since 5.0 we left it here since it has a default
     * implementation depending on {@link JetInstance#getHazelcastInstance()}.
     * Prefer to use {@link JetService#getJobStateSnapshots()} instead.
     */
    @Nonnull
    @Override
    @Deprecated
    default Collection<JobStateSnapshot> getJobStateSnapshots() {
        return getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshot(getHazelcastInstance(), (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(toList());
    }

    /**
     * @since Jet 4.4
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getSql()} instead.
     */
    @Beta
    @Nonnull
    @Deprecated
    SqlService getSql();

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getMap(String)} instead.
     */
    @Nonnull
    @Deprecated
    <K, V> IMap<K, V> getMap(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getReplicatedMap(String)} instead.
     */
    @Nonnull
    @Deprecated
    <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getList(String)} instead.
     */
    @Nonnull
    @Deprecated
    <E> IList<E> getList(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getReliableTopic(String)} instead.
     */
    @Nonnull
    @Deprecated
    <E> ITopic<E> getReliableTopic(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#getCacheManager()} instead.
     */
    @Deprecated
    @Nonnull
    JetCacheManager getCacheManager();

    /**
     * @deprecated since 5.0
     * Use {@link HazelcastInstance#shutdown()} instead.
     */
    @Deprecated
    void shutdown();
}
