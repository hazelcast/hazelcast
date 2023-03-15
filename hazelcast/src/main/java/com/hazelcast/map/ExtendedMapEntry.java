/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Interface to provide parity with IMap set and put operations. For use in EntryProcessors.
 *
 * @param <K> key type
 * @param <V> value type
 * @see com.hazelcast.map.IMap#set(Object, Object, long, TimeUnit)
 * @see com.hazelcast.map.IMap#put(Object, Object, long, TimeUnit)
 */
public interface ExtendedMapEntry<K, V> extends Entry<K, V> {

    /**
     * Set the value and set the TTL to a non-default value for the IMap
     */
    V setValue(V value, long ttl, TimeUnit ttlUnit);

    /**
     * Calling this method can result with one of these two cases:
     * <ul>
     *     <li>When there is no previous entry, this method
     *     works same as {@link Entry#setValue} and creates
     *     entry with existing expiry settings</li>
     *     <li>When an entry already exists, it updates
     *     value without changing expiry time of it</li>
     * </ul>
     * @since 5.2
     */
    V setValueWithoutChangingExpiryTime(V value);

}
