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

package com.hazelcast.jet.impl.application;


import com.hazelcast.jet.api.application.IOContext;
import com.hazelcast.jet.impl.data.io.DefaultObjectReaderFactory;
import com.hazelcast.jet.impl.data.io.DefaultObjectWriterFactory;
import com.hazelcast.jet.impl.data.io.Types;
import com.hazelcast.jet.spi.data.io.DataType;
import com.hazelcast.jet.spi.data.io.ObjectReaderFactory;
import com.hazelcast.jet.spi.data.io.ObjectWriterFactory;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;


public class IOContextImpl implements IOContext {
    private final ObjectReaderFactory objectReaderFactory;

    private final ObjectWriterFactory objectWriterFactory;

    private final List<Class> classes = new ArrayList<Class>();

    private final Map<Integer, DataType> types = new Int2ObjectHashMap<DataType>();

    private final Map<Class, DataType> classes2Types = new IdentityHashMap<Class, DataType>();

    public IOContextImpl() {
        this.objectReaderFactory = new DefaultObjectReaderFactory(this);
        this.objectWriterFactory = new DefaultObjectWriterFactory(this);
    }

    @Override
    public DataType getDataType(byte typeID) {
        DataType dataType = this.types.get((int) typeID);

        if (dataType == null) {
            return Types.getDataType(typeID);
        }

        return dataType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataType getDataType(Object object) {
        if (object == null) {
            return Types.NULL;
        }

        for (int idx = 0; idx < this.classes.size(); idx++) {
            Class clazz = this.classes.get(idx);

            if (clazz.isAssignableFrom(object.getClass())) {
                return this.classes2Types.get(clazz);
            }
        }

        return Types.getDataType(object);
    }

    @Override
    public void registerDataType(DataType dataType) {
        if (dataType.getTypeID() <= Types.NULL_TYPE_ID) {
            throw new IllegalStateException("Invalid typeId");
        }

        this.types.put((int) dataType.getTypeID(), dataType);
        this.classes2Types.put(dataType.getClazz(), dataType);
        this.classes.add(dataType.getClazz());
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.objectReaderFactory;
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.objectWriterFactory;
    }
}
