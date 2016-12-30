/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.jet.JetInstance;

abstract class AbstractJetInstance implements JetInstance {
    private static final String JET_ID_GENERATOR_NAME = "__jet_id_generator";

    private final HazelcastInstance hazelcastInstance;

    AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Cluster getCluster() {
        return getHazelcastInstance().getCluster();
    }

    @Override
    public String getName() {
        return hazelcastInstance.getName();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Override
    public <K, V> IMap<K, V> getMap(String name) {
        return hazelcastInstance.getMap(name);
    }

    @Override
    public <E> IList<E> getList(String name) {
        return hazelcastInstance.getList(name);
    }

    @Override
    public void shutdown() {
        hazelcastInstance.shutdown();
    }

    protected IdGenerator getIdGenerator() {
        return hazelcastInstance.getIdGenerator(JET_ID_GENERATOR_NAME);
    }
}
