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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.jet.impl.data.io.readers.BooleanReader;
import com.hazelcast.jet.impl.data.io.readers.ByteReader;
import com.hazelcast.jet.impl.data.io.readers.CharReader;
import com.hazelcast.jet.impl.data.io.readers.DefaultObjectReader;
import com.hazelcast.jet.impl.data.io.readers.DoubleReader;
import com.hazelcast.jet.impl.data.io.readers.FloatReader;
import com.hazelcast.jet.impl.data.io.readers.IntReader;
import com.hazelcast.jet.impl.data.io.readers.LongReader;
import com.hazelcast.jet.impl.data.io.readers.NullObjectReader;
import com.hazelcast.jet.impl.data.io.readers.ShortReader;
import com.hazelcast.jet.impl.data.io.readers.StringReader;
import com.hazelcast.jet.impl.data.io.readers.TupleReader;
import com.hazelcast.jet.impl.data.io.writers.BooleanWriter;
import com.hazelcast.jet.impl.data.io.writers.ByteWriter;
import com.hazelcast.jet.impl.data.io.writers.CharWriter;
import com.hazelcast.jet.impl.data.io.writers.DefaultObjectWriter;
import com.hazelcast.jet.impl.data.io.writers.DoubleWriter;
import com.hazelcast.jet.impl.data.io.writers.FloatWriter;
import com.hazelcast.jet.impl.data.io.writers.IntWriter;
import com.hazelcast.jet.impl.data.io.writers.LongWriter;
import com.hazelcast.jet.impl.data.io.writers.NullObjectWriter;
import com.hazelcast.jet.impl.data.io.writers.ShortWriter;
import com.hazelcast.jet.impl.data.io.writers.StringWriter;
import com.hazelcast.jet.impl.data.io.writers.TupleWriter;
import com.hazelcast.jet.impl.data.tuple.DefaultJetTupleFactory;
import com.hazelcast.jet.spi.data.io.DataType;
import com.hazelcast.jet.spi.data.io.ObjectReader;
import com.hazelcast.jet.spi.data.io.ObjectWriter;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.IdentityHashMap;
import java.util.Map;

//CHECKSTYLE:OFF
public enum Types implements DataType {
    BOOLEAN((byte) -11, Boolean.class, new BooleanWriter(), new BooleanReader()),
    CHAR((byte) -10, Character.class, new CharWriter(), new CharReader()),
    FLOAT((byte) -9, Float.class, new FloatWriter(), new FloatReader()),
    DOUBLE((byte) -8, Double.class, new DoubleWriter(), new DoubleReader()),
    SHORT((byte) -7, Short.class, new ShortWriter(), new ShortReader()),
    BYTE((byte) -6, Byte.class, new ByteWriter(), new ByteReader()),
    INT((byte) -5, Integer.class, new IntWriter(), new IntReader()),
    LONG((byte) -4, Long.class, new LongWriter(), new LongReader()),
    TUPLE((byte) -3,
            Tuple.class,
            new TupleWriter(),
            new TupleReader(new DefaultJetTupleFactory())
    ),
    STRING((byte) -2,
            String.class,
            new StringWriter(),
            new StringReader()
    ),
    OBJECT((byte) -1,
            Object.class,
            new DefaultObjectWriter(),
            new DefaultObjectReader()
    ),
    NULL(NULL_TYPE_ID,
            null,
            new NullObjectWriter(),
            new NullObjectReader()
    );

    private static final Class[] CLASSES;

    private static final Map<Class, DataType> CLASSES2_TYPES = new IdentityHashMap<Class, DataType>();

    private static final Map<Integer, DataType> TYPES = new Int2ObjectHashMap<DataType>();

    static {
        CLASSES = new Class[Types.values().length - 2];
        int i = 0;

        for (Types type : Types.values()) {
            if (type.getClazz() == null || (type.getClazz().equals(Object.class))) {
                continue;
            }

            TYPES.put((int) type.getTypeID(), type);
            CLASSES2_TYPES.put(type.getClazz(), type);
            CLASSES[i] = type.getClazz();
            i++;
        }
    }

    private final byte typeID;
    private final Class clazz;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    Types(byte typeID,
          Class clazz,
          ObjectWriter objectWriter,
          ObjectReader objectReader) {
        this.clazz = clazz;
        this.typeID = typeID;
        this.objectWriter = objectWriter;
        this.objectReader = objectReader;
    }

    public static DataType getDataType(byte typeID) {
        DataType dataType = TYPES.get((int) typeID);

        if (dataType == null) {
            dataType = OBJECT;
        }

        return dataType;
    }

    public static DataType getDataType(Object object) {
        if (object == null) {
            return Types.NULL;
        }

        for (Class<?> clazz : CLASSES) {
            if (clazz.isAssignableFrom(object.getClass())) {
                return CLASSES2_TYPES.get(clazz);
            }
        }

        return Types.OBJECT;
    }

    @Override
    public Class getClazz() {
        return this.clazz;
    }

    @Override
    public byte getTypeID() {
        return this.typeID;
    }

    @Override
    public ObjectWriter getObjectWriter() {
        return this.objectWriter;
    }

    @Override
    public ObjectReader getObjectReader() {
        return this.objectReader;
    }
}
//CHECKSTYLE:ON
