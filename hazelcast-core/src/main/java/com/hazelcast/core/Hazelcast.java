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

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class Hazelcast {

    private Hazelcast() {

    }

    public static <E> IQueue<E> getQueue(String name) {
        return com.hazelcast.impl.FactoryImpl.getQueue(name);
    }

    public static <E> ITopic<E> getTopic(String name) {
        return com.hazelcast.impl.FactoryImpl.getTopic(name);
    }

    public static <E> ISet<E> getSet(String name) {
        return com.hazelcast.impl.FactoryImpl.getSet(name);
    }

    public static <E> IList<E> getList(String name) {
        return com.hazelcast.impl.FactoryImpl.getList(name);
    }

    public static <K, V> IMap<K, V> getMap(String name) {
        return com.hazelcast.impl.FactoryImpl.getMap(name);
    }

    public static <K, V> MultiMap<K, V> getMultiMap(String name) {
        return com.hazelcast.impl.FactoryImpl.getMultiMap(name);
    }

    public static ILock getLock(Object obj) {
        return com.hazelcast.impl.FactoryImpl.getLock(obj);
    }

    public static Cluster getCluster() {
        return com.hazelcast.impl.FactoryImpl.getCluster();
    }

    public static ExecutorService getExecutorService() {
        return com.hazelcast.impl.FactoryImpl.getExecutorService();
    }

    public static Transaction getTransaction() {
        return com.hazelcast.impl.FactoryImpl.getTransaction();
    }

    public static IdGenerator getIdGenerator(String name) {
        return com.hazelcast.impl.FactoryImpl.getIdGenerator(name);
    }

    /**
     * Detaches currently running JVM from the cluster.
     * It doesn't shutdown the entire cluster, it shuts down
     * this local member only.
     */
    public static void shutdown() {
        com.hazelcast.impl.FactoryImpl.shutdown();
    }

    /**
     * Returns all queue, map, set, list, topic, lock, multimap
     * instances created by Hazelcast.
     *
     * @return the collection of instances created by Hazelcast.
     */
    public static Collection<ICommon> getInstances() {
        return com.hazelcast.impl.FactoryImpl.getInstances();
    }


    public static void addInstanceListener (InstanceListener instanceListener) {
        com.hazelcast.impl.FactoryImpl.addInstanceListener (instanceListener);
    }

    public static void removeInstanceListener (InstanceListener instanceListener) {
        com.hazelcast.impl.FactoryImpl.removeInstanceListener (instanceListener);
    }
}
