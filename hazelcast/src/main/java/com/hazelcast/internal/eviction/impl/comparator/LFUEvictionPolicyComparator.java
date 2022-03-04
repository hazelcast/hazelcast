/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction.impl.comparator;

import com.hazelcast.spi.eviction.EvictableEntryView;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.serialization.SerializableByConvention;

/**
 * {@link com.hazelcast.config.EvictionPolicy#LFU}
 * policy based {@link EvictionPolicyComparator}.
 */
@SerializableByConvention
public class LFUEvictionPolicyComparator
        implements EvictionPolicyComparator<Object, Object, EvictableEntryView<Object, Object>> {

    public static final LFUEvictionPolicyComparator INSTANCE
            = new LFUEvictionPolicyComparator();

    @Override
    public int compare(EvictableEntryView e1, EvictableEntryView e2) {
        int result = Long.compare(e1.getHits(), e2.getHits());
        // if hits are same, we try to select oldest entry to evict
        return result == 0 ? Long.compare(e1.getCreationTime(), e2.getCreationTime()) : result;
    }

    @Override
    public String toString() {
        return "LFUEvictionPolicyComparator{" + super.toString() + "} ";
    }

    @Override
    public final boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        return getClass().equals(o.getClass());
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode();
    }
}
