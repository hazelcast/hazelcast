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
}
