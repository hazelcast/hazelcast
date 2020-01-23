/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.Data;

/**
 * Adapter of arbitrary store. This adapter is used to pass record store to the index store.
 */
public interface StoreAdapter<R> {

    /**
     * Checks whether the record is expired and evicts it if so. Updates the record's last access time
     * if it is not evicted.
     *
     * @param key    key in data format
     * @param record the record to check
     * @param now    the current time
     * @param backup {@code true} if a backup partition, otherwise {@code false}.
     * @return {@code true} is the record is expired and evicted, otherwise {@code false}.
     */
    boolean evictIfExpired(Data key, R record, long now, boolean backup);

    /**
     * Checks whether ttl or maxIdle are set on the record.
     *
     * @param record the record to be checked
     * @return {@code true} if ttl or maxIdle are defined on the {@code record}, otherwise {@code false}.
     */
    boolean isTtlOrMaxIdleDefined(R record);

    /**
     * @return {@code true} if the store has at least one candidate entry
     * for expiration, otherwise {@code false}.
     */
    boolean isExpirable();

}
