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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

public class EntryModel {
    private static final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final DictionaryConfig config;
    private final List<FieldModel> valueFields = new ArrayList<>();
    private final List<FieldModel> keyFields = new ArrayList<>();
    private int valueLength;
    private int keyLength;
    private long valueObjectHeaderSize;

    public EntryModel(DictionaryConfig config) {
        this.config = config;
        initKey(config.getKeyClass());
        if (keyFields.isEmpty()) {
            throw new IllegalStateException("Key class " + config.getValueClass() + " has no instance fields");
        }
        initValue(config.getValueClass());
        if (valueFields.isEmpty()) {
            throw new IllegalStateException("Value class " + config.getValueClass() + " has no instance fields");
        }
        System.out.println(valueFields);
    }

    public long valueObjectHeaderSize() {
        return valueObjectHeaderSize;
    }

    public Collection<FieldModel> valueFields() {
        return valueFields;
    }

    public Class keyClass() {
        return config.getKeyClass();
    }

    public String keyClassName() {
        return keyClass().getName();
    }

    public Class valueClass() {
        return config.getValueClass();
    }

    public String valueClassName() {
        return valueClass().getName();
    }

    public String dictionaryName() {
        return config.getName();
    }

    public int keyLength() {
        return keyLength;
    }

    public int valueLength() {
        return valueLength;
    }

    public boolean fixedLengthKey() {
        return true;
    }

    public boolean fixedLengthValue() {
        return true;
    }

    public boolean fixedLengthEntry() {
        return fixedLengthKey() && fixedLengthValue();
    }

    public int entryLength() {
        return keyLength() + valueLength();
    }

    private void initValue(Class clazz) {
        long end = 0;
        long minFieldOffset = Long.MAX_VALUE;
        long maxFieldOffset = 0;
        do {
            for (Field f : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }

                long fieldOffset = unsafe.objectFieldOffset(f);

                FieldModel fieldModel = new FieldModel(f, fieldOffset);
                valueFields.add(fieldModel);

                if (fieldOffset > maxFieldOffset) {
                    maxFieldOffset = fieldOffset;
                    //System.out.println("fieldOffset:" + fieldOffset + " field.name:" + f.getName());
                    end = fieldOffset + fieldModel.size();
                }

                if (fieldOffset < minFieldOffset) {
                    minFieldOffset = fieldOffset;
                }
            }
        } while ((clazz = clazz.getSuperclass()) != null);

        // System.out.println("minFieldOffset:" + minFieldOffset);
        // System.out.println("maxFieldOffset:" + maxFieldOffset);
        // System.out.println("end:" + end);

        this.valueLength = (int) (end - minFieldOffset);
        this.valueObjectHeaderSize = minFieldOffset;
    }

    private void initKey(Class clazz) {
        long end = 0;
        long minFieldOffset = Long.MAX_VALUE;
        long maxFieldOffset = 0;
        do {
            for (Field f : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }

                long fieldOffset = unsafe.objectFieldOffset(f);

                FieldModel fieldModel = new FieldModel(f, fieldOffset);
                keyFields.add(fieldModel);

                if (fieldOffset > maxFieldOffset) {
                    maxFieldOffset = fieldOffset;
                    //System.out.println("fieldOffset:" + fieldOffset + " field.name:" + f.getName());
                    end = fieldOffset + fieldModel.size();
                }

                if (fieldOffset < minFieldOffset) {
                    minFieldOffset = fieldOffset;
                }
            }
        } while ((clazz = clazz.getSuperclass()) != null);

        // System.out.println("minFieldOffset:" + minFieldOffset);
        // System.out.println("maxFieldOffset:" + maxFieldOffset);
        // System.out.println("end:" + end);

        this.keyLength = (int) (end - minFieldOffset);
    }


}
