/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.serialization.SerializationService;

/**
 * Policy for merging data structure entries after a split-brain has been healed.
 *
 * @since 3.10
 */
public interface SplitBrainMergePolicy {

    /**
     * Selects one of the merging and existing data structure entries to be merged.
     * <p>
     * Note that as mentioned also in arguments, the {@link SplitBrainMergeEntryView} instance that represents
     * the existing data structure entry may be {@code null} if there is no existing entry for the specified key
     * in the {@link SplitBrainMergeEntryView} instance that represents the merging data structure entry.
     *
     * @param mergingEntry  {@link SplitBrainMergeEntryView} instance that has the data structure entry to be merged
     * @param existingEntry {@link SplitBrainMergeEntryView} instance that has the existing data structure entry
     *                      or {@code null} if there is no existing data structure entry
     * @param <K>           the type of the key
     * @param <V>           the type of the value
     * @return the selected value for merging
     */
    <K, V> V merge(SplitBrainMergeEntryView<K, V> mergingEntry, SplitBrainMergeEntryView<K, V> existingEntry);

    /**
     * Sets the {@link SerializationService} for this merge policy.
     * <p>
     * The keys and values of merging and existing {@link SplitBrainMergeEntryView}s are always in the in-memory format of the
     * backing data structure. This can be a serialized format, so the content cannot be processed without deserialization.
     * For most merge policies this will be fine, since the key or value are not used. If your implementation needs the
     * deserialized data, you can use {@link SerializationService#toObject(Object)} of the injected SerializationService.
     * <p>
     * The deserialization is not done eagerly for two main reasons:
     * <ul>
     * <li>The deserialization is quite expensive and should be avoided, if the result is not needed.</li>
     * <li>There is no need to locate classes of stored entries on the server side, when the entries are not deserialized.
     * So you can put entries from a client by using {@link com.hazelcast.config.InMemoryFormat#BINARY} with a different
     * classpath on client and server. In this case a deserialization could throw a {@link java.lang.ClassNotFoundException}.</li>
     * </ul>
     *
     * @param serializationService the {@link SerializationService}
     */
    void setSerializationService(SerializationService serializationService);
}
