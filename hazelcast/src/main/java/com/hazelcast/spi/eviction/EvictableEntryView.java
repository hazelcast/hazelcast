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

package com.hazelcast.spi.eviction;

/**
 * Contract point (from the end user perspective)
 * for serving/accessing entries that can be evicted.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface EvictableEntryView<K, V> {

    /**
     * Gets the key of the entry.
     *
     * @return the key of the entry
     */
    K getKey();

    /**
     * Gets the value of the entry.
     *
     * @return the value of the entry
     */
    V getValue();

    /**
     * Gets the creation time of this {@link EvictableEntryView} in milliseconds.
     *
     * @return the creation time of this {@link EvictableEntryView} in milliseconds
     */
    long getCreationTime();

    /**
     * Gets the latest access time difference of this {@link EvictableEntryView} in milliseconds.
     *
     * @return the latest access time of this {@link EvictableEntryView} in milliseconds
     */
    long getLastAccessTime();

    /**
     * Gets the access hit count of this {@link EvictableEntryView}.
     *
     * @return the access hit count of this {@link EvictableEntryView}
     */
    long getHits();
}
