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

package com.hazelcast.jet.impl.execution;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

/**
 * A Map.Entry implementation which implements {@link BroadcastItem}.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public final class BroadcastEntry<K, V> extends SimpleImmutableEntry<K, V> implements BroadcastItem {

    private static final long serialVersionUID = 1L;

    public BroadcastEntry(K key, V value) {
        super(key, value);
    }

    public BroadcastEntry(Entry<? extends K, ? extends V> entry) {
        super(entry);
    }

    @Override
    public String toString() {
        return getKey() + "=" + getValue();
    }
}
