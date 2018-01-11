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

package com.hazelcast.core;

/**
 * The IdGenerator is responsible for creating unique {@code long} IDs in a cluster, under normal operating scenarios.
 * <p>
 * In theory a {@link com.hazelcast.core.IAtomicLong#incrementAndGet()} could be used to provide the same functionality.
 * The big difference is that the {@code incrementAndGet} requires one or more remote calls for every invocation and therefore
 * is a performance and scalability bottleneck. The IdGenerator uses an {@link IAtomicLong} under the hood, but instead of
 * doing a remote call for every call to {@link #newId()}, it does it less frequently. It checks out a chunk of 10,000 IDs
 * and as long as it has not yet consumed all the IDs in its chunk, no remote call is done.
 * <p>
 * It can be that IDs generated by different cluster members will get out of order because each member will get its own
 * chunk. It can be that member 1 has chunk 1..10000 and member 2 has 10001..20000. Therefore, member 2 will automatically
 * have IDs that are out of order with the IDs generated by member 1. Next, the chunk is kept until all IDs are used.
 * <p>
 * It should be noted that IdGenerator does not guarantee unique IDs in a split brain situation. During a split brain,
 * different partitions of the cluster may have the primary or back-up, and will therefore continue in their respective
 * clusters from the last known value.  Other partitions of the cluster may not have the original primary nor the
 * back-up, in which case the {@link IdGenerator} will be created as new and initialised as per the provided
 * {@link #init(long)}
 * <p>
 * When a cluster heals after a split brain, a default large side merge takes place for the {@link IAtomicLong}
 * that keeps track of the last issued block of IDs, therefore it is possible that already issued IDs could be issued
 * again after the cluster is repaired. As a precaution when in a new record insert situation try to detect if the key
 * already exists and then grab another, possibly executing this logic only after a known split brain merge has occurred.
 * <p>
 * Care should be taken if you are using the IdGenerator to provide keys for {@link IMap} or
 * {@link com.hazelcast.cache.ICache}, as after split brain it could be possible for two unique values to share
 * the same key. When a {@link com.hazelcast.map.merge.MapMergePolicy} is applied, one of these unique values will
 * have to be discarded. Unless configured, the default merge policy is
 * {@link com.hazelcast.map.merge.PutIfAbsentMapMergePolicy}.
 * <p>
 * It is NOT RECOMMENDED to use {@link IdGenerator} to provide keys that are also used as unique identifiers in
 * underlying persistent storage, for example in an {@link IMap} that writes to a relational database using {@link
 * MapStore}.
 *
 * @deprecated The implementation can produce duplicate IDs in case of network split, even with split-brain
 * protection enabled (during short window while split-brain is detected). Use {@link
 * HazelcastInstance#getReliableIdGenerator(String)} for an alternative implementation which does not suffer
 * from this problem.
 */
@Deprecated
public interface IdGenerator extends DistributedObject {

    /**
     * Tries to initialize this {@code IdGenerator} instance with the given ID.
     * <p>
     * The next generated ID will be 1 greater than the supplied ID.
     *
     * @return {@code true} if initialization succeeded, {@code false} otherwise.
     */
    boolean init(long id);

    /**
     * Generates and returns a cluster-wide unique ID.
     *
     * @return new cluster-wide unique ID
     */
    long newId();
}
