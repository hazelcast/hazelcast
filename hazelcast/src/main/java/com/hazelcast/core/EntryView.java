/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * EntryView represents a readonly view of a map entry.
 *
 * @param <K> key
 * @param <V> value
 */
public interface EntryView<K, V> {

    /**
     * Returns the key of the entry.
     *
     * @return key
     */
    K getKey();

    /**
     * Returns the value of the entry.
     *
     * @return value
     */
    V getValue();

    /**
     * Returns the cost (in bytes) of the entry.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return cost in bytes
     */
    long getCost();

    /**
     * Returns the creation time of the entry.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return creation time
     */
    long getCreationTime();

    /**
     * Returns the expiration time of the entry.
     *
     * @return expiration time
     */
    long getExpirationTime();

    /**
     * Returns number of hits of the entry.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return hits
     */
    long getHits();

    /**
     * Returns the last access time to the entry.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return last access time
     */
    long getLastAccessTime();

    /**
     * Returns the last time value is flushed to mapstore.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return last store time
     */
    long getLastStoredTime();

    /**
     * Returns the last time value is updated.
     *
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     * @return last update time
     */
    long getLastUpdateTime();

    /**
     * Returns the version of the entry
     *
     * @return version
     */
    long getVersion();
}
