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

package com.hazelcast.core;

/**
 * Represents a read-only view of a data structure entry.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface EntryView<K, V> {

    /**
     * Returns the key of the entry.
     *
     * @return the key of the entry
     */
    K getKey();

    /**
     * Returns the value of the entry.
     *
     * @return the value of the entry
     */
    V getValue();

    /**
     * Returns the cost (in bytes) of the entry.
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return the cost in bytes of the entry
     */
    long getCost();

    /**
     * Returns the creation time of the entry.
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return the creation time of the entry
     */
    long getCreationTime();

    /**
     * Returns the expiration time of the entry.
     *
     * @return the expiration time of the entry
     */
    long getExpirationTime();

    /**
     * Returns number of hits of the entry.
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return number of hits of the entry
     */
    long getHits();

    /**
     * Returns the last access time for the entry.
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return the last access time for the entry
     */
    long getLastAccessTime();

    /**
     * Returns the last time the value was flushed to its store (e.g. {@link MapStore}).
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return the last store time for the value
     */
    long getLastStoredTime();

    /**
     * Returns the last time the value was updated.
     * <p>
     * <b>Warning:</b> This method returns {@code -1} if statistics are not enabled or not implemented.
     *
     * @return the last time the value was updated
     */
    long getLastUpdateTime();

    /**
     * Returns the version of the entry.
     *
     * @return the version of the entry
     */
    long getVersion();

    /**
     * Returns the last set time to live second.
     *
     * @return the last set time to live second
     */
    long getTtl();
}
