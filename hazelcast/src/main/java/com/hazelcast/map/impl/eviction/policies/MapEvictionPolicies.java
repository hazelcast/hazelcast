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

import com.hazelcast.config.EvictionPolicy;

/**
 * Used to get out-of-the-box {@link MapEvictionPolicy} instances
 */
public final class MapEvictionPolicies {

    private static final MapEvictionPolicy LRU_POLICY = new LRUMapEvictionPolicy();

    private static final MapEvictionPolicy LFU_POLICY = new LFUMapEvictionPolicy();

    private static final MapEvictionPolicy NULL_POLICY = new NullMapEvictionPolicy();

    private MapEvictionPolicies() {
    }


    public static MapEvictionPolicy getMapEvictionPolicy(EvictionPolicy evictionPolicy) {

        switch (evictionPolicy) {
            case LFU:
                return LFU_POLICY;
            case LRU:
                return LRU_POLICY;
            case NONE:
                return NULL_POLICY;
            default:
                throw new IllegalArgumentException("Unknown map eviction policy : " + evictionPolicy);
        }
    }
}
