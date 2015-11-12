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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.SizeEstimator;

import java.util.Collection;

/**
 * Represents actual storage layer behind a {@link RecordStore}.
 * Includes basic storage operations.
 *
 * @param <K> the key type to be put in this storage.
 * @param <R> the value type to be put in this storage.
 */
public interface Storage<K, R> {

    void put(K key, R record);

    void updateRecordValue(K key, R record, Object value);

    R get(K key);

    void removeRecord(R record);

    boolean containsKey(K key);

    Collection<R> values();

    int size();

    boolean isEmpty();

    void clear();

    void destroy();

    SizeEstimator getSizeEstimator();

    void setSizeEstimator(SizeEstimator sizeEstimator);

    void dispose();
}
