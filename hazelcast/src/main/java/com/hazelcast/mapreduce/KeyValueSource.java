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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

public abstract class KeyValueSource<K, V> implements Cloneable {

    public abstract void open(HazelcastInstance hazelcastInstance);

    public abstract boolean hasNext();

    public abstract KeyValuePair<K, V> next();

    public abstract boolean reset();

    public static <K, V> KeyValueSource<K, V> fromMap(IMap<K, V> map) {
        return null;
    }

    public static <K, V> KeyValueSource<K, V> fromMultiMap(MultiMap<K, V> multiMap) {
        return null;
    }
}
