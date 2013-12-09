/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>A ReplicatedMap is a map like data structure with non-strong consistency
 * (so-called eventually consistent) and values are locally stored on every
 * node of the cluster. When accessing values, due to the eventually
 * consistency, it is possible to read staled data since replication
 * is not handled in a synchronous way.</p>
 * <p>Whenever a value is written asynchronously the new value will be internally
 * distributed to all existing cluster members and eventually every node will have
 * the new value and the cluster again is in a consistent state.</p>
 * <p>When a new node joins the cluster the new node initially will request existing
 * values from older nodes and replicate those locally.</p>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface ReplicatedMap<K, V> extends Map<K, V>, DistributedObject {

    /**
     * <p>Associates a given value to the specified key and replicates it to the
     * cluster. If there is an old value it will be replaced by the specified
     * one and is returned from the call.</p>
     * <p>In addition you have to specify a ttl and it's {@link TimeUnit}
     * to define when the value is outdated and should be removed from the
     * replicated map.</p>
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param ttl ttl to be associated with the specified key-value pair
     * @param timeUnit TimeUnit to be used for the ttl value
     */
    V put(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * <p>The clear operation is not supported by Hazelcast since it can result
     * in an inconsistent state of the ReplicatedMap.</p>
     * <p>This operation can be achieved using a distributed call to
     * {@link DistributedObject#destroy()} and wait for all distributed objects are
     * destroyed before recreating those distributed objects on all nodes.</p>
     */
    void clear();

}
