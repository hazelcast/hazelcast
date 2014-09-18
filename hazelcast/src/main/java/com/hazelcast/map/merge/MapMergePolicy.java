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

package com.hazelcast.map.merge;

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * A policy for merging maps after a splitbrain was detected and the different network partitions need
 * to be merged.
 *
 * @see com.hazelcast.map.merge.MapMergePolicy
 * @see com.hazelcast.map.merge.PutIfAbsentMapMergePolicy
 * @see com.hazelcast.map.merge.LatestUpdateMapMergePolicy
 * @see com.hazelcast.map.merge.PassThroughMergePolicy
 */
public interface MapMergePolicy extends DataSerializable {

    /**
     * Returns the value of the entry after the merge
     * of entries with the same key.
     * You should consider the case where existingEntry's value is null.
     *
     * @param mapName       name of the map
     * @param mergingEntry  entry merging into the destination cluster
     * @param existingEntry existing entry in the destination cluster
     * @return final value of the entry. If returns null then entry will be removed.
     */
    Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry);

}
