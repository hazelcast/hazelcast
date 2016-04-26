/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * A policy for merging replicated maps after a split-brain was detected and the different network partitions need
 * to be merged.
 *
 * @see com.hazelcast.replicatedmap.merge.HigherHitsMapMergePolicy
 * @see com.hazelcast.replicatedmap.merge.PutIfAbsentMapMergePolicy
 * @see com.hazelcast.replicatedmap.merge.LatestUpdateMapMergePolicy
 * @see com.hazelcast.replicatedmap.merge.PassThroughMergePolicy
 */
public interface ReplicatedMapMergePolicy extends Serializable {

    /**
     * Returns the value of the entry after the merge
     * of entries with the same key.
     * You should consider the case where existingEntry's value is null.
     *
     * @param mapName       name of the replicated map
     * @param mergingEntry  entry merging into the destination cluster
     * @param existingEntry existing entry in the destination cluster
     * @return final value of the entry. If returns null, then the entry will be removed.
     */
    Object merge(String mapName, ReplicatedMapEntryView mergingEntry, ReplicatedMapEntryView existingEntry);

}
