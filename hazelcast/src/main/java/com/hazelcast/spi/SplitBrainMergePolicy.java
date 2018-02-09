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

package com.hazelcast.spi;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.merge.MergeDataHolder;
import com.hazelcast.spi.serialization.SerializationServiceAware;

/**
 * Policy for merging data structure entries after a split-brain has been healed.
 * <p>
 * The values of merging and existing {@link MergeDataHolder}s are always in the in-memory format of the backing data structure.
 * This can be a serialized format, so the content cannot be processed without deserialization.
 * For most merge policies this will be fine, since the key or value are not used.
 * If your implementation needs the deserialized data, you can use {@link SerializationServiceAware}
 * to get the {@link com.hazelcast.spi.serialization.SerializationService} injected.
 * <p>
 * The deserialization is not done eagerly for two main reasons:
 * <ul>
 * <li>The deserialization is quite expensive and should be avoided, if the result is not needed.</li>
 * <li>There is no need to locate classes of stored entries on the server side, when the entries are not deserialized.
 * So you can put entries from a client by using {@link com.hazelcast.config.InMemoryFormat#BINARY} with a different
 * classpath on client and server. In this case a deserialization could throw a {@link java.lang.ClassNotFoundException}.</li>
 * </ul>
 *
 * @since 3.10
 */
public interface SplitBrainMergePolicy extends DataSerializable {

    /**
     * Selects one of the merging and existing data structure entries to be merged.
     * <p>
     * Note that as mentioned also in arguments, the {@link MergeDataHolder} instance that represents
     * the existing data structure entry may be {@code null} if there is no existing entry for the specified key
     * in the {@link MergeDataHolder} instance that represents the merging data structure entry.
     *
     * @param mergingData  {@link MergeDataHolder} instance that has the data structure entry to be merged
     * @param existingData {@link MergeDataHolder} instance that has the existing data structure entry
     *                     or {@code null} if there is no existing data structure entry
     * @param <T>          the type of the value
     * @return the selected value for merging
     */
    <T> T merge(MergeDataHolder<T> mergingData, MergeDataHolder<T> existingData);
}
