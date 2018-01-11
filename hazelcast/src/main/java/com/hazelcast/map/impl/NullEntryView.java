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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;

/**
 * Contains only key no value.
 *
 * @param <K>
 * @param <V>
 */
class NullEntryView<K, V> implements EntryView<K, V> {

    private K key;

    NullEntryView(K key) {
        this.key = key;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return null;
    }

    @Override
    public long getCost() {
        return 0;
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public long getExpirationTime() {
        return 0;
    }

    @Override
    public long getHits() {
        return 0;
    }

    @Override
    public long getLastAccessTime() {
        return 0;
    }

    @Override
    public long getLastStoredTime() {
        return 0;
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public long getTtl() {
        return 0;
    }

}
