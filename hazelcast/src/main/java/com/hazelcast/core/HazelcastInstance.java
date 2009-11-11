/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import com.hazelcast.config.Config;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public interface HazelcastInstance {

    public String getName();

    public <E> IQueue<E> getQueue(String name);

    public <E> ITopic<E> getTopic(String name);

    public <E> ISet<E> getSet(String name);

    public <E> IList<E> getList(String name);

    public <K, V> IMap<K, V> getMap(String name);

    public <K, V> MultiMap<K, V> getMultiMap(String name);

    public ILock getLock(Object obj);

    public Cluster getCluster();

    public ExecutorService getExecutorService();

    public Transaction getTransaction();

    public IdGenerator getIdGenerator(String name);

    /**
     * Detaches this member from the cluster.
     * It doesn't shutdown the entire cluster, it shuts down
     * this local member only.
     */
    public void shutdown();

    /**
     * Detaches this member from the cluster first and then restarts it
     * as a new member.
     */
    public void restart();

    /**
     * Returns all queue, map, set, list, topic, lock, multimap
     * instances created by Hazelcast.
     *
     * @return the collection of instances created by Hazelcast.
     */
    public Collection<Instance> getInstances();

    public void addInstanceListener(InstanceListener instanceListener);

    public void removeInstanceListener(InstanceListener instanceListener);

    public Config getConfig();
}
