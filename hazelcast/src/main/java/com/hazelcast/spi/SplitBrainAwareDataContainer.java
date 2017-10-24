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

/**
 * Interface for split-brain capable data containers of services, which implement {@link SplitBrainHandlerService}.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @param <T> the type of the merged item
 * @since 3.10
 */
public interface SplitBrainAwareDataContainer<K, V, T> {

    /**
     * Merges the given {@link SplitBrainMergeEntryView} with an existing entry via the supplied {@link SplitBrainMergePolicy}.
     * <p>
     * The existing entry has to be retrieved by the implementing data container, via the key or value of the merging entry.
     * Both entries are passed to {@link SplitBrainMergePolicy#merge(SplitBrainMergeEntryView, SplitBrainMergeEntryView)},
     * which decides if one of the entries should be merged.
     * <p>
     * The return type of this methods depends on the contract between the calling merge operation and the data container, e.g.
     * <ul>
     * <li>the merged entry instance in the data container specific format, to create a backup operation</li>
     * <li>{@code null} to signal that nothing has been merged</li>
     * <li>{@code boolean} to signal if the {@code mergingEntry} was merged or not</li>
     * </ul>
     *
     * @param mergingEntry the {@link SplitBrainMergeEntryView} instance that wraps key/value for merging and existing entry
     * @param mergePolicy  the {@link SplitBrainMergePolicy} instance for handling merge policy
     * @return the internal item representation to create a backup operation if merge was applied, otherwise {@code null}
     */
    T merge(SplitBrainMergeEntryView<K, V> mergingEntry, SplitBrainMergePolicy mergePolicy);
}
