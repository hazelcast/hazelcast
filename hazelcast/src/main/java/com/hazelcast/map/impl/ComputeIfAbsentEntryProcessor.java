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
import java.util.function.Function;

public class ComputeIfAbsentEntryProcessor<K, V> implements EntryProcessor<K, V, V>, IdentifiedDataSerializable {

    Function<? super K, ? extends V> function;

    public ComputeIfAbsentEntryProcessor() {
    }

    public ComputeIfAbsentEntryProcessor(Function<? super K, ? extends V> function) {
        this.function = function;
    }

    @Override
    public V process(Map.Entry<K, V> entry) {
        V currentValue = entry.getValue();
        if (currentValue != null) {
            return currentValue;
        }

        V newValue = function.apply(entry.getKey());
        if (newValue == null) {
            return null;
        }

        entry.setValue(newValue);
        return newValue;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.COMPUTE_IF_ABSENT_PROCESSOR;
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
