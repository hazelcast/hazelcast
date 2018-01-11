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

package com.hazelcast.internal.eviction.impl.comparator;

import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.nio.serialization.SerializableByConvention;

/**
 * {@link com.hazelcast.config.EvictionPolicy#RANDOM} policy based {@link EvictionPolicyComparator}.
 */
@SerializableByConvention
public class RandomEvictionPolicyComparator extends EvictionPolicyComparator {

    @Override
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    public int compare(EvictableEntryView e1, EvictableEntryView e2) {
        return 0;
    }
}
