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
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.dictionary.impl.type.Kind.FIXED_LENGTH_RECORD;
import static com.hazelcast.dictionary.impl.type.Kind.PRIMITIVE;
import static com.hazelcast.dictionary.impl.type.Kind.PRIMITIVE_ARRAY;
import static com.hazelcast.dictionary.impl.type.Kind.PRIMITIVE_WRAPPER;
import static com.hazelcast.dictionary.impl.type.Kind.STRING;
import static com.hazelcast.dictionary.impl.type.Kind.UNKNOWN;
import static com.hazelcast.dictionary.impl.type.Kind.VARIABLE_LENGTH_RECORD;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static java.util.Arrays.asList;

/**
 *
 */
public class Type {

    private static final List<Class> PRIMITIVE_TYPES = asList(
            boolean.class,
            byte.class,
            short.class,
            char.class,
            int.class,
            long.class,
            float.class,
            double.class);

    private static final List<Class> PRIMITIVE_WRAPPER_TYPES = asList(
            Boolean.class,
            Byte.class,
            Short.class,
            Character.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class);

    private static final List<Class> PRIMITIVE_ARRAY_TYPES = asList(
            boolean[].class,
            byte[].class,
            short[].class,
            char[].class,
            int[].class,
            long[].class,
            float[].class,
            double[].class);

    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    private final List<ClassField> fixedLengthFields = new ArrayList<>();
    private final List<ClassField> variableLengthFields = new ArrayList<>();
    private final Class type;
    private long objectHeaderSize;
    private int fixedLength;
    private Kind kind;

    public Type(Class type) {
        this.type = type;


        if (String.class.equals(type)) {
            kind = STRING;
        } else if (PRIMITIVE_ARRAY_TYPES.contains(type)) {
            kind = PRIMITIVE_ARRAY;
        } else {
            List<Field> fields = collectFields(type);
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("No suitable fields found on class [" + clazz().getName() + "]");
            }
            long offheapOffset = 0;
            for (Field f : fields) {
                Kind kind = toKind(f.getType());
                if (kind == UNKNOWN) {
                    throw new IllegalArgumentException("Unsupported field [" + f + "]");
                }

                if (kind.isFixedLength()) {
                    ClassField field = new ClassField(f, kind, offheapOffset);
                    fixedLengthFields.add(field);
                    offheapOffset += field.sizeOffheap();
                } else {
                    ClassField field = new ClassField(f, kind, -1);
                    variableLengthFields.add(field);
                }
            }

            this.fixedLength = calcFixedLength();
            this.objectHeaderSize = objectHeaderSize(fields);
            this.kind = variableLengthFields().size() > 0 ? VARIABLE_LENGTH_RECORD : FIXED_LENGTH_RECORD;
        }
    }

    public Class arrayElementClass() {
        if (kind != Kind.PRIMITIVE_ARRAY) {
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

    public Kind kind() {
        return kind;
    }

    public List<ClassField> fixedLengthFields() {
        return fixedLengthFields;
    }

    public Class clazz() {
        return type;
    }

    public String name() {
        if (kind == Kind.PRIMITIVE_ARRAY) {
            return arrayElementClass() + "[]";
        }

        return clazz().getName().replace("$", ".");
    }

    public long objectHeaderSize() {
        return objectHeaderSize;
    }

    public int fixedLength() {
        return fixedLength;
    }

    public List<ClassField> variableLengthFields() {
        return variableLengthFields;
    }

    public boolean isFixedLength() {
        return kind.isFixedLength();
    }

    private Kind toKind(Class clazz) {
        if (PRIMITIVE_TYPES.contains(clazz)) {
            return PRIMITIVE;
        } else if (PRIMITIVE_WRAPPER_TYPES.contains(clazz)) {
            return PRIMITIVE_WRAPPER;
        } else if (PRIMITIVE_ARRAY_TYPES.contains(clazz)) {
            return PRIMITIVE_ARRAY;
        } else {
            return UNKNOWN;
        }
    }

    private static List<Field> collectFields(Class clazz) {
        List<Field> fields = new LinkedList<>();
        do {
            for (Field f : clazz.getDeclaredFields()) {
                if (isStatic(f.getModifiers())) {
                    continue;
                }

                if (isTransient(f.getModifiers())) {
                    continue;
                }

                fields.add(f);
            }
            clazz = clazz.getSuperclass();
        } while (clazz != null);

        return fields;
    }

    private static long objectHeaderSize(List<Field> fields) {
        long result = Integer.MAX_VALUE;
        for (Field field : fields) {
            long heapOffset = UNSAFE.objectFieldOffset(field);

            if (heapOffset < result) {
                result = heapOffset;
            }
        }
        return result;
    }

    private int calcFixedLength() {
        int result = 0;
        for (ClassField field : fixedLengthFields) {
            result += field.sizeOffheap();
        }
        return result;
    }

    @Override
    public String toString() {
        return "Type{"
                + "name=" + name()
                + ", kind=" + kind
                + ", fixedLength=" + fixedLength
                + ", fixedLengthFields=" + fixedLengthFields
                + ", variableLengthFields=" + variableLengthFields
                + ", objectHeaderSize=" + objectHeaderSize
                + '}';
    }
}
