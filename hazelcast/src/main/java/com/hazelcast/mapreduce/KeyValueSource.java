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

package com.hazelcast.mapreduce;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.impl.MapKeyValueSource;
import com.hazelcast.mapreduce.impl.MultiMapKeyValueSource;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public abstract class KeyValueSource<K, V> implements Cloneable {

    public abstract void open(NodeEngine nodeEngine);

    public abstract boolean hasNext();

    public abstract Map.Entry<K, V> next();

    public abstract boolean reset();

    public final Collection<K> getAllKeys() {
        if (!isAllKeysSupported()) {
            throw new UnsupportedOperationException("getAllKeys is unsupported for this KeyValueSource");
        }
        return getAllKeys0();
    }

    public boolean isAllKeysSupported() {
        return false;
    }

    protected Collection<K> getAllKeys0() {
        return Collections.emptyList();
    }

    public static <K, V> KeyValueSource<K, V> fromMap(IMap<K, V> map) {
        return new MapKeyValueSource<K, V>(map.getName());
    }

    public static <K, V> KeyValueSource<K, V> fromMultiMap(MultiMap<K, V> multiMap) {
        return new MultiMapKeyValueSource<K, V>(multiMap.getName());
    }
}
