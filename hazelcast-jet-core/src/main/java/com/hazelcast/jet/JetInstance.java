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

package com.hazelcast.jet;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;

/**
 * Javadoc pending
 */
public interface JetInstance {

    /**
     * @return javadoc pending
     */
    String getName();

    /**
     * @return javadoc pending
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Javadoc pending
     *
     * @return
     */
    Cluster getCluster();

    /**
     * @return javadoc pending
     */
    JetConfig getConfig();

    /**
     * @return javadoc pending
     */
    Job newJob(DAG dag);

    /**
     * @return javadoc pending
     */
    Job newJob(DAG dag, JobConfig config);

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    <K, V> IStreamMap<K, V> getMap(String name);

    /**
     * Returns the distributed list instance with the specified name.
     * Index based operations on the list are not supported.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    <E> IStreamList<E> getList(String name);

    /**
     * Javadoc pending
     */
    void shutdown();

}
