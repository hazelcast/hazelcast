/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import java.util.Map;

/**
 * Extends {@link EntryProcessor} by allowing the object returned by
 * its sole method to be of a generic type R. This allows implementations
 * to return objects of a specific type from processing IMap entries
 * without the need for casting.
 * @param <K> Type of key of a {@link java.util.Map.Entry}
 * @param <V> Type of value of a {@link java.util.Map.Entry}
 * @param <R> Type of object returned by {@link #process(Map.Entry)}
 * @see EntryProcessor
 */
public interface TypedEntryProcessor<K, V, R> extends EntryProcessor<K, V> {
    R process(Map.Entry<K, V> entry);
}
