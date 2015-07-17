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

package com.hazelcast.map.impl.mapstore.writebehind.entry;

/**
 * General contract for an entry to be stored into {@link com.hazelcast.core.MapStore}
 * when {@link com.hazelcast.config.MapStoreConfig#writeDelaySeconds} is greater than 0.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @see AddedDelayedEntry
 * @see DeletedDelayedEntry
 * @see NullValueDelayedEntry
 */
public interface DelayedEntry<K, V> {

    K getKey();

    V getValue();

    long getStoreTime();

    int getPartitionId();

    long getSequence();

    void setStoreTime(long storeTime);

    void setSequence(long sequence);
}
