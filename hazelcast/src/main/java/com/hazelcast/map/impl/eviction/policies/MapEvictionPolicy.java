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

package com.hazelcast.map.impl.eviction.policies;

import com.hazelcast.core.EntryView;

/**
 * Used to plug {@link com.hazelcast.core.IMap} eviction policy.
 *
 * @see LRUMapEvictionPolicy
 * @see LFUMapEvictionPolicy
 */
public interface MapEvictionPolicy {

    /**
     * Returns number of samples used to find eviction candidate among them
     *
     * @return sample count
     */
    int getSampleCount();

    /**
     * Returns candidate to be evicted.
     *
     * @param samples samples used to find eviction candidate among them
     * @return candidate to be evicted
     */
    EntryView selectEvictableEntry(Iterable<EntryView> samples);

    /**
     * Returns {@code true} if supplied candidate will be the new selected entry to evict, otherwise returns {@code false}
     *
     * @param selected  currently selected entry to evict
     * @param candidate candidate entry to evict which will be compared with selected one
     * @return {@code true} if candidate will be the new selected entry to evict, otherwise {@code false}
     */
    boolean compare(EntryView selected, EntryView candidate);
}
