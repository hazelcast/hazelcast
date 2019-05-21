/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;

import java.util.Collection;

/**
 * Hazelcast distributed map implementation is an in-memory data store
 * but it can be backed by any type of data store such as RDBMS, OODBMS,
 * or simply a file based data store.
 * <p>
 * {@link com.hazelcast.core.IMap#get(Object)} normally returns the
 * value that is available in-memory. If the entry doesn't exist
 * in-memory, Hazelcast returns {@code null}. If a loader implementation
 * is provided then, instead of returning {@code null}, Hazelcast will
 * attempt to load the unknown entry by calling the implementation's
 * {@link #load(Object)} or {@link #loadAll(Collection)}  methods.
 * Loaded entries will be placed into the distributed map and they will
 * stay in-memory until they are explicitly removed or implicitly
 * evicted (if eviction is configured). Entries may have expiration
 * time attached to them. In that case, the entry is removed after
 * given expiration time.
 * <p>
 * EntryLoader implementations are executed by a partition thread,
 * therefore care should be taken not to block the thread with an
 * expensive operation or an operation that may potentially never
 * return, the partition thread does not time out the operation. Whilst
 * the partition thread is executing the EntryLoader it is unable to
 * respond to requests for data on any other structure that may reside
 * in the same partition, or to respond to other partitions mapped to
 * the same partition thread. For example a very slow EntryLoader for
 * one map could block a request for data on another map, or even a
 * queue. It is therefore strongly recommended not to use EntryLoader
 * to call across a WAN or to a system which will take on average longer
 * than a few milliseconds to respond.
 * <p>
 * EntryLoaders should not be used to perform cascading operations on
 * other data structures via a {@link HazelcastInstance}, the EntryLoader
 * should only concern itself with the operation on the assigned map. If
 * the EntryLoader attempts to access another data structure on a
 * different partition to the key used in the EntryLoader, a
 * {@link IllegalThreadStateException} is thrown. An EntryLoader can only
 * interact with other data structures that reside on the same partition.
 * <p>
 * If a blocked partition thread is called from a Hazelcast Client the
 * caller will also block indefinitely, for example {@link com.hazelcast.core.IMap#get(Object)}.
 * If the same call is made from another cluster member the operation
 * will eventually timeout with a {@link com.hazelcast.core.OperationTimeoutException}.
 *
 * @param <K> type of the EntryLoader key
 * @param <V> type of the EnyryLoader value
 */
public interface EntryLoader<K, V> extends MapLoader<K, EntryLoaderEntry<V>> {
}
