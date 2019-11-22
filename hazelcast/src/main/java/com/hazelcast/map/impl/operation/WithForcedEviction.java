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

package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.properties.HazelcastProperty;

public final class WithForcedEviction {

    public static final int DEFAULT_FORCED_EVICTION_RETRY_COUNT = 5;
    public static final String PROP_FORCED_EVICTION_RETRY_COUNT
              = "hazelcast.internal.forced.eviction.retry.count";
    public static final HazelcastProperty FORCED_EVICTION_RETRY_COUNT
              = new HazelcastProperty(PROP_FORCED_EVICTION_RETRY_COUNT,
                                      DEFAULT_FORCED_EVICTION_RETRY_COUNT);

    static final ForcedEviction[] EVICTIONS = new ForcedEviction[]{
        new RecordStoreForcedEviction(),
        new PartitionRecordStoreForcedEviction(),
        new AllEntriesForcedEviction(),
        new PartitionAllEntriesForcedEviction()
    };

    private WithForcedEviction() {
    }
}
