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

import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class IOContextImpl implements IOContext {
    private final List<Class> classes = new ArrayList<>();
    private final Map<Integer, DataType> types = new Int2ObjectHashMap<>();
    private final Map<Class, DataType> classes2Types = new IdentityHashMap<>();

    public IOContextImpl(DataType... systemDataTypes) {
        for (DataType dataType : systemDataTypes) {
            addTypes(dataType);
        }
    }

    @Override
    public DataType lookupDataType(byte typeID) {
        DataType dataType = this.types.get((int) typeID);
        return dataType == null ? PredefinedType.getDataType(typeID) : dataType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataType resolveDataType(Object object) {
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

    @Override
    public void registerDataType(DataType dataType) {
        if (dataType.typeId() <= DataType.NULL_TYPE_ID) {
            throw new IllegalStateException("Invalid typeId");
        }
        addTypes(dataType);
    }

    private void addTypes(DataType dataType) {
        this.types.put((int) dataType.typeId(), dataType);
        this.classes2Types.put(dataType.getClazz(), dataType);
        this.classes.add(dataType.getClazz());
    }
}
