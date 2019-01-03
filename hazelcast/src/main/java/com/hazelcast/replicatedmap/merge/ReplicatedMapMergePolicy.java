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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;

import java.io.Serializable;

/**
 * Policy for merging replicated map entries after a split-brain has been healed.
 *
 * @see com.hazelcast.replicatedmap.merge.HigherHitsMapMergePolicy
 * @see com.hazelcast.replicatedmap.merge.LatestUpdateMapMergePolicy
 * @see com.hazelcast.replicatedmap.merge.PassThroughMergePolicy
 * @see com.hazelcast.replicatedmap.merge.PutIfAbsentMapMergePolicy
 */
public interface ReplicatedMapMergePolicy extends Serializable {

    /**
     * Selects one of the merging and existing map entries to be merged.
     * <p>
     * Note that the {@code existingEntry} may be {@code null} if there
     * is no entry with the same key in the destination map.
     * This happens, when the entry for that key was
     * <ul>
     * <li>only created in the smaller sub-cluster during the split-brain</li>
     * <li>removed in the larger sub-cluster during the split-brain</li>
     * </ul>
     *
     * @param mapName       name of the replicated map
     * @param mergingEntry  {@link ReplicatedMapEntryView} instance that has the map entry to be merged
     * @param existingEntry {@link ReplicatedMapEntryView} instance that has the existing map entry
     *                      or {@code null} if there is no existing map entry
     * @return the selected value for merging or {@code null} if the entry should be removed
     */
    Object merge(String mapName, ReplicatedMapEntryView mergingEntry, ReplicatedMapEntryView existingEntry);
}
