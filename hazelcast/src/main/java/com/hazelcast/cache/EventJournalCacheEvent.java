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

package com.hazelcast.cache;

/**
 * The event for the cache event journal.
 *
 * @param <K> the entry key type
 * @param <V> the entry value type
 */
public interface EventJournalCacheEvent<K, V> {
    /**
     * Returns the key for the event entry.
     *
     * @return the entry key
     */
    K getKey();

    /**
     * Returns the new value for the event entry. In some cases this
     * is {@code null} while in other cases it may be non-{@code null}. For instance,
     * when the event is of type {@link CacheEventType#CREATED}, the new
     * value is non-{@code null} but when it is of type {@link CacheEventType#REMOVED},
     * the value is {@code null}.
     *
     * @return the entry new value
     */
    V getNewValue();

    /**
     * Returns the old value for the event entry. In some cases this
     * is {@code null} while in other cases it may be non-{@code null}. For instance,
     * when the event is of type {@link CacheEventType#CREATED}, the old
     * value is {@code null} but when it is of type {@link CacheEventType#REMOVED},
     * the value is non-{@code null}.
     *
     * @return the entry old value
     */
    V getOldValue();

    /**
     * Returns the event type.
     *
     * @return the event type
     */
    CacheEventType getType();
}
