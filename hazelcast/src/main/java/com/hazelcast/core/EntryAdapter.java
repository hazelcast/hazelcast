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
 * Adapter for EntryListener.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry.
 * @see com.hazelcast.core.EntryListener
 */
public class EntryAdapter<K, V> implements EntryListener<K, V> {

    public void entryAdded(EntryEvent<K, V> event) {
    }

    public void entryRemoved(EntryEvent<K, V> event) {
    }

    public void entryUpdated(EntryEvent<K, V> event) {
    }

    public void entryEvicted(EntryEvent<K, V> event) {
    }
}
