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
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return the cost in bytes of the entry
     */
    long getCost();

    /**
     * Returns the creation time of the entry.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns -1 if statistics is not enabled.
     * </p>
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
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return number of hits of the entry
     */
    long getHits();

    /**
     * Returns the last access time for the entry.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return the last access time for the entry
     */
    long getLastAccessTime();

    /**
     * Returns the last time the value was flushed to mapstore.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return the last store time for the value
     */
    long getLastStoredTime();

    /**
     * Returns the last time the value was updated.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method returns -1 if statistics is not enabled.
     * </p>
     *
     * @return the last time the value was updated
     */
    long getLastUpdateTime();

    /**
     * Returns the version of the entry
     *
     * @return the version of the entry
     */
    long getVersion();

    /**
     * Returns the last set time to live second.
     *
     * @return the last set time to live second.
     */
    long getTtl();
}
