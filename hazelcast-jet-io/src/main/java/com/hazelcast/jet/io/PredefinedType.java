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
import java.util.IdentityHashMap;
import java.util.Map;

public enum PredefinedType implements DataType {
    BOOLEAN(-12, Boolean.class, new BooleanIO()),
    CHAR(-11, Character.class, new CharIO()),
    FLOAT(-10, Float.class, new FloatIO()),
    DOUBLE(-9, Double.class, new DoubleIO()),
    SHORT(-8, Short.class, new ShortIO()),
    BYTE(-7, Byte.class, new ByteIO()),
    INT(-6, Integer.class, new IntegerIO()),
    LONG(-5, Long.class, new LongIO()),
    // -4 Reserved for JetTuple
    TUPLE2(-3, Pair.class, new Tuple2IO()),
    STRING(-2, String.class, new StringIO()),
    OBJECT(-1, Object.class, new DefaultObjectIO()),
    NULL(NULL_TYPE_ID, null, new NullObjectIO());

    private static final Class[] CLASSES;

    private static final Map<Class, DataType> CLASSES2_TYPES = new IdentityHashMap<>();

    private static final Map<Integer, DataType> TYPES = new Int2ObjectHashMap<>();

    static {
        CLASSES = new Class[PredefinedType.values().length - 2];
        int i = 0;
        for (PredefinedType type : PredefinedType.values()) {
            if (type.getClazz() == null || (type.getClazz().equals(Object.class))) {
                continue;
            }
            TYPES.put((int) type.typeId(), type);
            CLASSES2_TYPES.put(type.getClazz(), type);
            CLASSES[i] = type.getClazz();
            i++;
        }
    }

    private final byte typeId;
    private final Class clazz;
    private final ObjectIO objectIO;

    PredefinedType(int typeId, Class clazz, ObjectIO objectIO) {
        this.clazz = clazz;
        this.typeId = (byte) typeId;
        this.objectIO = objectIO;
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
            return PredefinedType.NULL;
        }
        for (Class<?> clazz : CLASSES) {
            if (clazz.isAssignableFrom(object.getClass())) {
                return CLASSES2_TYPES.get(clazz);
            }
        }
        return PredefinedType.OBJECT;
    }

    @Override
    public Class getClazz() {
        return this.clazz;
    }

    @Override
    public byte typeId() {
        return this.typeId;
    }

    @Override
    public void write(Object o, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
        objectIO.write(o, objectDataOutput, ioContext);
    }

    @Override
    public Object read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
        return objectIO.read(objectDataInput, ioContext);
    }


    private interface ObjectIO<T> {
        T read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException;

        void write(T object, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException;
    }

    private static class BooleanIO implements ObjectIO<Boolean> {
        @Override
        public Boolean read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readBoolean();
        }

        @Override
        public void write(Boolean b, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(BOOLEAN.typeId());
            objectDataOutput.writeBoolean(b);
        }
    }

    private static class ByteIO implements ObjectIO<Byte> {
        @Override
        public Byte read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readByte();
        }

        @Override
        public void write(Byte b, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(BYTE.typeId());
            objectDataOutput.write(b);
        }
    }

    private static class CharIO implements ObjectIO<Character> {
        @Override
        public Character read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readChar();
        }

        @Override
        public void write(Character c, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(CHAR.typeId());
            objectDataOutput.writeChar(c);
        }
    }

    private static class DefaultObjectIO implements ObjectIO {
        @Override
        public Object read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readObject();
        }

        @Override
        public void write(Object o, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(OBJECT.typeId());
            objectDataOutput.writeObject(o);
        }
    }

    private static class DoubleIO implements ObjectIO<Double> {
        @Override
        public Double read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readDouble();
        }

        @Override
        public void write(Double d, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(DOUBLE.typeId());
            objectDataOutput.writeDouble(d);
        }
    }

    private static class FloatIO implements ObjectIO<Float> {
        @Override
        public Float read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readFloat();
        }

        @Override
        public void write(Float f, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(FLOAT.typeId());
            objectDataOutput.writeFloat(f);
        }
    }

    private static class IntegerIO implements ObjectIO<Integer> {
        @Override
        public Integer read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readInt();
        }

        @Override
        public void write(Integer i, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(INT.typeId());
            objectDataOutput.writeInt(i);
        }
    }

    private static class LongIO implements ObjectIO<Long> {
        @Override
        public Long read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readLong();
        }

        @Override
        public void write(Long l, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(LONG.typeId());
            objectDataOutput.writeLong(l);
        }
    }

    private static class NullObjectIO implements ObjectIO {
        @Override
        public Object read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readObject();
        }

        @Override
        public void write(Object o, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(NULL.typeId());
            objectDataOutput.writeObject(o);
        }
    }

    private static class ShortIO implements ObjectIO<Short> {
        @Override
        public Short read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readShort();
        }

        @Override
        public void write(Short i, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(SHORT.typeId());
            objectDataOutput.writeShort(i);
        }
    }

    private static class StringIO implements ObjectIO<String> {
        @Override
        public String read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return objectDataInput.readUTF();
        }

        @Override
        public void write(String s, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(STRING.typeId());
            objectDataOutput.writeUTF(s);
        }
    }

    private static class Tuple2IO implements ObjectIO<Pair> {
        @Override
        public Pair read(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            return new Pair<>(readComponent(objectDataInput, ioContext), readComponent(objectDataInput, ioContext));
        }

        @Override
        public void write(Pair tuple, ObjectDataOutput objectDataOutput, IOContext ioContext) throws IOException {
            objectDataOutput.writeByte(TUPLE2.typeId());
            for (int i = 0; i < 2; i++) {
                final Object o = tuple.get(i);
                ioContext.resolveDataType(o).write(o, objectDataOutput, ioContext);
            }
        }

        private static Object readComponent(ObjectDataInput objectDataInput, IOContext ioContext) throws IOException {
            final byte typeID = objectDataInput.readByte();
            return ioContext.lookupDataType(typeID).read(objectDataInput, ioContext);
        }
    }
}
