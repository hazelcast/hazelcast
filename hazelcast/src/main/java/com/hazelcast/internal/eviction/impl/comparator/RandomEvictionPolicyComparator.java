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

package com.hazelcast.internal.eviction.impl.comparator;

import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * {@link com.hazelcast.config.EvictionPolicy#RANDOM} policy based {@link EvictionPolicyComparator}.
 */
@SuppressFBWarnings(
        value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
        justification = "No need to serializable since its instance is not serialized")
public class RandomEvictionPolicyComparator extends EvictionPolicyComparator {

    @Override
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    public int compare(EvictableEntryView e1, EvictableEntryView e2) {
        return 0;
    }
}
