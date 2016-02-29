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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.eviction.policies.MapEvictionPolicy;

/**
 * Contains common methods to implement {@link MapEvictionPolicy}
 */
public abstract class AbstractMapEvictionPolicy implements MapEvictionPolicy {

    /**
     * Default sample count to collect
     */
    private static final int DEFAULT_SAMPLE_COUNT = 15;

    @Override
    public EntryView selectEvictableEntry(Iterable<EntryView> samples) {
        EntryView selected = null;

        for (EntryView candidate : samples) {
            if (selected == null) {
                selected = candidate;
            } else if (compare(selected, candidate)) {
                selected = candidate;
            }
        }

        return selected;
    }

    @Override
    public int getSampleCount() {
        return DEFAULT_SAMPLE_COUNT;
    }
}
