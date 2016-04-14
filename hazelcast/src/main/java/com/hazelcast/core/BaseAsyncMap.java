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

package com.hazelcast.core;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Base interface for a {@link BaseMap} providing additional {@link Future}-returning
 * *Async methods.
 */
public interface BaseAsyncMap<K, V> extends BaseMap<K, V> {

    Future<V> getAsync(K key);

    Future<V> putAsync(K key, V value);

    Future<V> putAsync(K key, V value, long ttl, TimeUnit timeunit);

    Future<Void> setAsync(K key, V value);

    Future<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit);

    Future<V> removeAsync(K key);
}
