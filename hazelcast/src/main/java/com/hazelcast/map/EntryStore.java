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

package com.hazelcast.map;

import com.hazelcast.map.EntryLoader.MetadataAwareValue;

/**
 * This is an extension to {@link MapStore}. Implementing classes can
 * retrieve expiration dates of entries stored if there is any.
 *
 * See {@link MapStore}.
 *
 * @param <K> type of the EntryStore key
 * @param <V> type of the EntryStore value
 */
public interface EntryStore<K, V> extends EntryLoader<K, V>, MapStore<K, MetadataAwareValue<V>> {
}
