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

package com.hazelcast.internal.eviction;

/**
 * Enum for eviction policy types.
 * @deprecated since 3.9, use {@link com.hazelcast.config.EvictionPolicy} instead
 */
@Deprecated
public enum EvictionPolicyType {

    /**
     * Least Recently Used
     */
    LRU,

    /**
     * Least Frequently Used
     */
    LFU,

    /**
     * Picks a random entry
     */
    RANDOM,

    /**
     * Doesn't evict entries (will not add new entries to the Near Cache when it's full)
     */
    NONE,
}
