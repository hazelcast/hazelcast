/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector.map;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Extension of Map interface that provides async methods needed for the Hazelcast 3 connector
 * <p>
 * It is required to access async methods of Hazelcast 3 IMap.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface AsyncMap<K, V> extends Map<K, V> {

    /**
     * Delegates to IMap#getAsync(K)
     */
    CompletionStage<V> getAsync(@Nonnull K key);

    /**
     * Asynchronously copies all of the mappings from the specified map to this map.
     */
    CompletionStage<Void> putAllAsync(Map<? extends K, ? extends V> items);
}
