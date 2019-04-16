/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

public class RecordModel {

    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final Map<String, Field> fields = new HashMap<String, Field>();
    private final Class recordClass;
    private final LinkedList<String> indexFields;
    private final int indexSize;
    private long dataOffset;
    private int payloadSize;
    private int size;

    public RecordModel(Class recordClass, Set<String> indexFields) {
        this.recordClass = recordClass;
        this.indexFields = new LinkedList<>(indexFields);
        initRecordData(recordClass);
        //System.out.println("record size:" + recordDataSize);
        this.indexSize = calculateIndicesSize();
    }

    public int offsetNextForIndex(String index) {
        // each record has a next pointer per index. It points to the next record with the same index value.
        return indexFields.indexOf(index) * INT_SIZE_IN_BYTES + payloadSize;
    }

    public int getIndexSize() {
        return indexSize;
    }

    private int calculateIndicesSize() {
        int indexSize = 0;
        for (String index : indexFields) {
            indexSize += indexSize(index);
        }
        return indexSize;
    }

    public int indexStartOffset(String fieldName) {
        int indexStartOffset = 0;
        for (String index : indexFields) {
            if (index.equals(fieldName)) {
                return indexStartOffset;
            }
            indexStartOffset += indexSize(index);
        }
        throw new RuntimeException(fieldName + " is not an index");
    }

    public int indexSize(String fieldName) {
        Field field = getField(fieldName);
        Class fieldType = field.getType();
        if (fieldType.equals(Boolean.TYPE)) {
            return 2 * INT_SIZE_IN_BYTES;
        } else if (fieldType.equals(Byte.TYPE)) {
            return 256 * INT_SIZE_IN_BYTES;
        } else if (fieldType.equals(Short.TYPE)
                || fieldType.equals(Character.TYPE)
                || fieldType.equals(Integer.TYPE)
                || fieldType.equals(Long.TYPE)
                || fieldType.equals(Float.TYPE)
                || fieldType.equals(Double.TYPE)) {
            return 64 * 1024;
        } else {
            throw new RuntimeException("Unrecognized field:" + field);
        }
    }

    public List<String> getIndexFields() {
        return indexFields;
    }

    public Class getRecordClass() {
        return recordClass;
    }

    public String getRecordClassName() {
        return getRecordClass().getName().replace("$", ".");
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public int getSize() {
        return size;
    }

    private void initRecordData(Class clazz) {
        long end = 0;
        long minFieldOffset = Long.MAX_VALUE;
        long maxFieldOffset = 0;
        do {
            for (Field f : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }

                fields.put(f.getName(), f);

                long fieldOffset = unsafe.objectFieldOffset(f);

                if (fieldOffset > maxFieldOffset) {
                    maxFieldOffset = fieldOffset;
                    //System.out.println("fieldOffset:" + fieldOffset + " field.name:" + f.getName());
                    end = fieldOffset + fieldSize(f);
                }

                if (fieldOffset < minFieldOffset) {
                    minFieldOffset = fieldOffset;
                }
            }
        } while ((clazz = clazz.getSuperclass()) != null);

        // System.out.println("minFieldOffset:" + minFieldOffset);
        // System.out.println("maxFieldOffset:" + maxFieldOffset);
        // System.out.println("end:" + end);

        this.payloadSize = (int) (end - minFieldOffset);
        this.size = payloadSize + INT_SIZE_IN_BYTES * indexFields.size();
        this.dataOffset = minFieldOffset;
    }

    private static int fieldSize(Field field) {
        if (field.getType().equals(Byte.TYPE)) {
            return 1;
        } else if (field.getType().equals(Integer.TYPE)) {
            return INT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Long.TYPE)) {
            return LONG_SIZE_IN_BYTES;
        } else if (field.getType().equals(Short.TYPE)) {
            return SHORT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Float.TYPE)) {
            return FLOAT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Double.TYPE)) {
            return DOUBLE_SIZE_IN_BYTES;
        } else if (field.getType().equals(Boolean.TYPE)) {
            return BOOLEAN_SIZE_IN_BYTES;
        } else if (field.getType().equals(Character.TYPE)) {
            return CHAR_SIZE_IN_BYTES;
        } else {
            throw new RuntimeException(
                    "Unrecognized field type: field '" + field.getName() + "' ,type '" + field.getType().getName() + "' ");
        }
    }
}
