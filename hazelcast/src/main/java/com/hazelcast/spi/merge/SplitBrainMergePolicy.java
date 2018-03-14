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

package com.hazelcast.spi.merge;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeAware;

/**
 * Policy for merging data structure values after a split-brain has been healed.
 * <p>
 * The values of merging and existing {@link MergingValue}s are always in the in-memory format of the
 * backing data structure. This can be a serialized format, so the content cannot be processed without deserialization.
 * For most merge policies this will be fine, since the key or value are not used.
 * <p>
 * The deserialization is not done eagerly for two main reasons:
 * <ul>
 * <li>The deserialization is quite expensive and should be avoided, if the result is not needed.</li>
 * <li>There is no need to locate classes of stored entries on the server side, when the entries are not deserialized.
 * So you can put entries from a client by using {@link com.hazelcast.config.InMemoryFormat#BINARY} with a different
 * classpath on client and server. In this case a deserialization could throw a {@link java.lang.ClassNotFoundException}.</li>
 * </ul>
 * If you need the deserialized data you can call {@link MergingValue#getDeserializedValue()} or
 * {@link MergingEntry#getDeserializedKey()}, which will deserialize the data lazily.
 * <p>
 * A merge policy can implement {@link HazelcastInstanceAware} to get the {@link HazelcastInstance} injected.
 * This can be used to retrieve the user context via {@link HazelcastInstance#getUserContext()},
 * which is an easy way to get user dependencies that are otherwise hard to obtain.
 * <p>
 * A merge policy can also implement {@link NodeAware} to get an instance of {@link Node} injected.
 *
 * @since 3.10
 */
public interface SplitBrainMergePolicy extends DataSerializable {

    /**
     * Selects the value of either the merging or the existing {@link MergingValue} which should be merged.
     * <p>
     * Note that the existing {@link MergingValue} instance may be {@code null}
     * if no matching data could be found to the merging {@link MergingValue}.
     *
     * @param mergingValue  {@link MergingValue} instance that has the merging data of the smaller sub-cluster
     * @param existingValue {@link MergingValue} instance that has the existing data
     *                      or {@code null} if no matching data exists
     * @param <V>           the type of the value
     * @return the selected value for merging
     */
    <V> V merge(MergingValue<V> mergingValue, MergingValue<V> existingValue);
}
