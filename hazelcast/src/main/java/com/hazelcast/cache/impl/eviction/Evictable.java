/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

/**
 * Interface for entries, records or whatever that can be evicted.
 */
public interface Evictable {

    /**
     * Gets the creation time of this {@link Evictable} in milliseconds.
     *
     * @return the creation time of this {@link Evictable} in milliseconds
     */
    long getCreationTime();

    /**
     * Gets the latest access time difference of this {@link Evictable} in milliseconds.
     *
     * @return the latest access time of this {@link Evictable} in milliseconds
     */
    long getAccessTime();

    /**
     * Gets the access hit count of this {@link Evictable}.
     *
     * @return the access hit count of this {@link Evictable}
     */
    int getAccessHit();

}
