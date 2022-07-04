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

package com.hazelcast.map.impl;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

public class MapEntryReplacingEntryProcessor<K, V> implements EntryProcessor<K, V, V>, IdentifiedDataSerializable {

    BiFunction<? super K, ? super V, ? extends V> function;

    public MapEntryReplacingEntryProcessor() {
    }

    public MapEntryReplacingEntryProcessor(BiFunction<? super K, ? super V, ? extends V> function) {
        this.function = function;
    }

    @Override
    public V process(Map.Entry<K, V> entry) {
        V newValue = function.apply(entry.getKey(), entry.getValue());
        entry.setValue(newValue);
        return null;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_ENTRY_REPLACING_PROCESSOR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(function);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        function = in.readObject();
    }

}
