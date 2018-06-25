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

package com.hazelcast.jet.impl;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.stream.impl.IListDecorator;
import com.hazelcast.jet.stream.impl.IMapDecorator;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

abstract class AbstractJetInstance implements JetInstance {
    private final HazelcastInstance hazelcastInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;

    AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(this, null));
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

    protected long uploadResourcesAndAssignId(JobConfig config) {
        return jobRepository.get().uploadJobResources(config);
    }

}
