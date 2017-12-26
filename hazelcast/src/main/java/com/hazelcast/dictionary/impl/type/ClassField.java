/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl.type;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.Bits;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static com.hazelcast.dictionary.impl.type.Kind.PRIMITIVE_ARRAY;
import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * Represents a field of a class.
 */
public class ClassField {
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    private final Kind kind;
    private final Field field;
    private final long heapOffset;
    private final long offheapOffset;

    public ClassField(Field field, Kind kind, long offheapOffset) {
        this.field = field;
        this.heapOffset = UNSAFE.objectFieldOffset(field);
        this.offheapOffset = offheapOffset;
        this.kind = kind;
    }

    /**
     * Gets the type of the element in the array.
     *
     * So if the type is 'byte[]', then the result will be 'byte'.
     *
     * @return the type of the element.
     * @throws IllegalStateException if the Type is not referring to an array.
     */
    public Class arrayElementType() {
        if (kind != PRIMITIVE_ARRAY) {
            throw new IllegalStateException("Type " + this + " is not a primitive array");
        }

        Class type = clazz();
        if (boolean[].class.equals(type)) {
            return boolean.class;
        } else if (byte[].class.equals(type)) {
            return byte.class;
        } else if (short[].class.equals(type)) {
            return short.class;
        } else if (char[].class.equals(type)) {
            return char.class;
        } else if (int[].class.equals(type)) {
            return int.class;
        } else if (long[].class.equals(type)) {
            return long.class;
        } else if (float[].class.equals(type)) {
            return float.class;
        } else if (double[].class.equals(type)) {
            return double.class;
        } else {
            throw new RuntimeException("Unrecognized type:" + type);
        }
    }

    /**
     * Returns the name of the field.
     *
     * @return the name of the field.
     */
    public String name() {
        return field.getName();
    }

    /**
     * Returns the class of the field.
     *
     * @return the class of the field.
     */
    public Class clazz() {
        return field.getType();
    }

    /**
     * Returns the {@link Field}.
     *
     * @return the field.
     */
    public Field field() {
        return field;
    }

    /**
     * Returns the heapOffset of the field in the object.
     *
     * @return the heapOffset of the field in the object.
     */
    public long offsetHeap() {
        return heapOffset;
    }

    /**
     * Returns the offset of the object in offheap.
     *
     * @return
     */
    public long offsetOffheap() {
        return offheapOffset;
    }

    /**
     * Returns the {@link Kind} of this ClassField.
     *
     * @return the Kind
     */
    public Kind kind() {
        return kind;
    }

    /**
     * Returns the size in bytes of the field in the heap.
     *
     * In case of a primitive type, the size of the type is returned.
     *
     * In case of an object type, the size of the reference is returned.
     *
     * @return the size of the field on the heap.
     */
    public int sizeHeap() {
        return Bits.fieldSizeInBytes(clazz());
    }

    /**
     * Returns the size in bytes of the field in off-heap.
     *
     * In case of a primitive type, the size of the type is returned.
     *
     * In case of an primitive wrapper, the size of primitive + 1
     *
     * @return the size of the field on the heap.
     */
    public int sizeOffheap() {
        if (Boolean.TYPE.equals(clazz())) {
            return BOOLEAN_SIZE_IN_BYTES;
        } else if (Byte.TYPE.equals(clazz())) {
            return BYTE_SIZE_IN_BYTES;
        } else if (Character.TYPE.equals(clazz())) {
            return CHAR_SIZE_IN_BYTES;
        } else if (Short.TYPE.equals(clazz())) {
            return SHORT_SIZE_IN_BYTES;
        } else if (Integer.TYPE.equals(clazz())) {
            return INT_SIZE_IN_BYTES;
        } else if (Long.TYPE.equals(clazz())) {
            return LONG_SIZE_IN_BYTES;
        } else if (Float.TYPE.equals(clazz())) {
            return FLOAT_SIZE_IN_BYTES;
        } else if (Double.TYPE.equals(clazz())) {
            return DOUBLE_SIZE_IN_BYTES;
        } else if (field.getType().equals(Boolean.class)) {
            return BYTE_SIZE_IN_BYTES + BOOLEAN_SIZE_IN_BYTES;
        } else if (field.getType().equals(Byte.class)) {
            return BYTE_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES;
        } else if (field.getType().equals(Short.class)) {
            return BYTE_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Character.class)) {
            return BYTE_SIZE_IN_BYTES + CHAR_SIZE_IN_BYTES;
        } else if (field.getType().equals(Integer.class)) {
            return BYTE_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Long.class)) {
            return BYTE_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;
        } else if (field.getType().equals(Float.class)) {
            return BYTE_SIZE_IN_BYTES + FLOAT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Double.class)) {
            return BYTE_SIZE_IN_BYTES + DOUBLE_SIZE_IN_BYTES;
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public String toString() {
        return "ClassField{"
                + "field='" + field + "'"
                + ", kind=" + kind
                + ", type=" + clazz()
                + ", heapOffset=" + heapOffset
                + ", offheapOffset=" + offheapOffset
                + '}';
    }
}
