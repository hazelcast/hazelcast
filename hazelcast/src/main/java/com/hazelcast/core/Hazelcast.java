/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
import com.hazelcast.impl.FactoryImpl;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class Hazelcast {
    private static volatile HazelcastInstance defaultInstance = null;
    private final static Object initLock = new Object();

    private Hazelcast() {
    }

    public static void init(Config config) {
        if (defaultInstance != null) {
            throw new IllegalStateException("Default Hazelcast instance is already initilized.");
        }
        synchronized (initLock) {
            if (defaultInstance != null) {
                throw new IllegalStateException("Default Hazelcast instance is already initilized.");
            }
            defaultInstance = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
        }
    }

    private static HazelcastInstance getDefaultInstance() {
        if (defaultInstance == null) {
            synchronized (initLock) {
                if (defaultInstance == null) {
                    defaultInstance = com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(null);
                }
            }
        }
        return defaultInstance;
    }

    public static <E> IQueue<E> getQueue(String name) {
        return getDefaultInstance().getQueue(name);
    }

    public static <E> ITopic<E> getTopic(String name) {
        return getDefaultInstance().getTopic(name);
    }

    public static <E> ISet<E> getSet(String name) {
        return getDefaultInstance().getSet(name);
    }

    public static <E> IList<E> getList(String name) {
        return getDefaultInstance().getList(name);
    }

    public static <K, V> IMap<K, V> getMap(String name) {
        return getDefaultInstance().getMap(name);
    }

    public static <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getDefaultInstance().getMultiMap(name);
    }

    public static ILock getLock(Object obj) {
        return getDefaultInstance().getLock(obj);
    }

    public static Cluster getCluster() {
        return getDefaultInstance().getCluster();
    }

    public static ExecutorService getExecutorService() {
        return getDefaultInstance().getExecutorService();
    }

    public static Transaction getTransaction() {
        return getDefaultInstance().getTransaction();
    }

    public static IdGenerator getIdGenerator(String name) {
        return getDefaultInstance().getIdGenerator(name);
    }

    /**
     * Detaches this member from the cluster.
     * It doesn't shutdown the entire cluster, it shuts down
     * this local member only.
     */
    public static void shutdown() {
        synchronized (initLock) {
            if (defaultInstance != null) {
                getDefaultInstance().shutdown();
                defaultInstance = null;
            }
        }
    }

    /**
     * Shuts down all running Hazelcast Instances on this JVM, including the
     * default one if it is running. It doesn't shutdown all members of the
     * cluster but just the ones running on this JVM.
     *
     * @see #newHazelcastInstance(Config)
     */
    public static void shutdownAll() {
        com.hazelcast.impl.FactoryImpl.shutdownAll();
        defaultInstance = null;
    }

    /**
     * Detaches this member from the cluster first and then restarts it
     * as a new member.
     */
    public static void restart() {
        synchronized (initLock) {
            if (defaultInstance != null) {
                defaultInstance = com.hazelcast.impl.FactoryImpl.restart((FactoryImpl.HazelcastInstanceProxy) defaultInstance);
            } else {
                getDefaultInstance();
            }
        }
    }

    /**
     * Returns all queue, map, set, list, topic, lock, multimap
     * instances created by Hazelcast.
     *
     * @return the collection of instances created by Hazelcast.
     */
    public static Collection<Instance> getInstances() {
        return getDefaultInstance().getInstances();
    }

    public static void addInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().addInstanceListener(instanceListener);
    }

    public static void removeInstanceListener(InstanceListener instanceListener) {
        getDefaultInstance().removeInstanceListener(instanceListener);
    }

    /**
     * Creates a new Hazelcast instance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running hazelcast instances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new hazelcast instance (member)
     * @return new hazelcast instance
     * @see #shutdownAll()
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        return com.hazelcast.impl.FactoryImpl.newHazelcastInstanceProxy(config);
    }
}
