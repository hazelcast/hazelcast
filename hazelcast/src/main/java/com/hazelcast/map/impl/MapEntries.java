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

package com.hazelcast.map.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MapEntries is a collection of {@link Data} instances for keys and values of a {@link java.util.Map.Entry}.
 */
public interface MapEntries extends IdentifiedDataSerializable {

    void add(Data key, Data value);

    List<Map.Entry<Data, Data>> entries();

    Data getKey(int index);

    Data getValue(int index);

    int size();

    boolean isEmpty();

    void clear();

    void putAllToList(Collection<Map.Entry<Data, Data>> targetList);

    <K, V> void putAllToMap(SerializationService serializationService, Map<K, V> map);
}
