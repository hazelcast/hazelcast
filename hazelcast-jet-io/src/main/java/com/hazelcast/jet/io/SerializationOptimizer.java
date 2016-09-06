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

package com.hazelcast.jet.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around Hazelcast Serialization which optimizes the serialized format of primitive types and String.
 */
public class SerializationOptimizer {
    private final List<Class> classes = new ArrayList<>();
    private final Map<Integer, DataType> customTypes = new Int2ObjectHashMap<>();
    private final Map<Class, DataType> classes2Types = new IdentityHashMap<>();

    public SerializationOptimizer(DataType... customDataTypes) {
        for (DataType dataType : customDataTypes) {
            addCustomType(dataType);
        }
    }

    public void write(Object o, ObjectDataOutput objectDataOutput) throws IOException {
        resolve(o).write(o, objectDataOutput, this);
    }

    public Object read(ObjectDataInput objectDataInput) throws IOException {
        return lookup(objectDataInput.readByte()).read(objectDataInput, this);
    }

    private DataType lookup(byte typeID) {
        final DataType predefined = PredefinedType.getDataType(typeID);
        if (predefined == PredefinedType.OBJECT) {
            final DataType custom = customTypes.get((int) typeID);
            if (custom != null) {
                return custom;
            }
        }
        return predefined;
    }

    @SuppressWarnings("unchecked")
    private DataType resolve(Object object) {
        if (object == null) {
            return PredefinedType.NULL;
        }
        for (Class clazz : this.classes) {
            if (clazz.isAssignableFrom(object.getClass())) {
                return this.classes2Types.get(clazz);
            }
        }
        return PredefinedType.getDataType(object);
    }

    public void addCustomType(DataType dataType) {
        this.customTypes.put((int) dataType.typeId(), dataType);
        this.classes2Types.put(dataType.getClazz(), dataType);
        this.classes.add(dataType.getClazz());
    }
}

