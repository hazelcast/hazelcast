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
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;

/**
 * @since Jet 3.0
 *
 * @deprecated After 5.0 Jet was merged into core Hazelcast product. Jet
 * became a service of Hazelcast instead of being an instance of its own
 * that encapsulates {@link HazelcastInstance}. Please use {@link
 * JetService} instead.
 */
@Deprecated
public interface JetInstance extends JetService {

    /**
     * Returns the underlying Hazelcast instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     *
     * @since Jet 3.0
     *
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
