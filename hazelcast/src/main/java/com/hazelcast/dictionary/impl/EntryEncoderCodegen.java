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

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static java.lang.String.format;

public class EntryEncoderCodegen {
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final StringBuffer codeBuffer = new StringBuffer();
    private final EntryModel model;

    public EntryEncoderCodegen(EntryModel model) {
        this.model = model;
    }

    public EntryEncoderCodegen add(String s, Object... args) {
        codeBuffer.append(format(s, (Object[]) args));
        return this;
    }

    public EntryEncoderCodegen add(Object arg) {
        codeBuffer.append(arg);
        return this;
    }

    public String className() {
        return model.dictionaryName() + "_Encoder";
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    public void generate() {
        add("import java.util.*;\n");
        add("import com.hazelcast.util.*;\n");
        add("import com.hazelcast.dictionary.impl.*;\n\n\n");

        add("public final class %s\n", className());
        add("             extends com.hazelcast.dictionary.impl.EntryEncoder<%s, %s>{\n\n",
                model.keyClassName(), model.valueClassName());

        generateConstructor();
        generateWriteEntry();
        generateWriteKey();
        generateWriteValue();
        generateReadValue();
        generateReadKey();
        generateKeyMatches();
        add("}\n");
    }

    private void generateConstructor() {
        add("    public %s(EntryModel model){\n", className());
        add("        super(model);\n");
        add("    }\n\n");
    }

    private void generateWriteEntry() {
        add("    public int writeEntry(%s key, %s value, long address, int available){\n", model.keyClassName(), model.valueClassName());

        if (model.fixedLengthEntry()) {
            add("        if (available < %s) return -1;\n", model.entryLength());
            addPutKey("address");
            valueToOffheap("address+" + model.keyLength());
            add("        return %s;\n", model.entryLength());
        } else {
            throw new UnsupportedOperationException();
        }

        add("    }\n\n");
    }

    private void generateWriteKey() {
        add("    public long writeKey(%s key, long address){\n", model.keyClassName());
        if (model.fixedLengthKey()) {
            addPutKey("address");
            add("        return address + %s;\n", model.keyLength());
        } else {
            throw new UnsupportedOperationException();
        }
        add("    }\n\n");
    }

    private void addPutKey(String address) {
        Class key = model.keyClass();
        if (key == Long.class) {
            add("        unsafe.putLong(%s, key.longValue());\n", address);
        } else if (key == Integer.class) {
            add("        unsafe.putInt(%s, key.intValue());\n", address);
        } else if (key == Byte.class) {
            add("        unsafe.putByte(%s, key.byteValue());\n", address);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void valueToOffheap(String address) {
        for (FieldModel field : model.valueFields()) {
            Class fieldType = field.type();
            String toAddress = address + "+" + (field.offset() - model.valueObjectHeaderSize());
            if (fieldType.equals(Integer.TYPE)) {
                add("        unsafe.putInt(%s, unsafe.getInt(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Long.TYPE)) {
                add("        unsafe.putLong(%s, unsafe.getLong(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Float.TYPE)) {
                add("        unsafe.putFloat(%s, unsafe.getFloat(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Double.TYPE)) {
                add("        unsafe.putDouble(%s, unsafe.getDouble(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Character.TYPE)) {
                add("        unsafe.putChar(%s, unsafe.getChar(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Short.TYPE)) {
                add("        unsafe.putShort(%s, unsafe.getShort(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Boolean.TYPE)) {
                add("        unsafe.putBoolean(null, %s, unsafe.getBoolean(value, %s));\n", toAddress, field.offset());
            } else if (fieldType.equals(Byte.TYPE)) {
                add("        unsafe.putByte(%s, unsafe.getByte(value, %s));\n", toAddress, field.offset());
            }


//            addPutField(field, "        ", ),
//                    "value." + field.name());
        }
    }

    private void generateWriteValue() {
        add("    public long writeValue(%s value, long address){\n", model.valueClassName());
        if (model.fixedLengthValue()) {
            valueToOffheap("address");
            add("        return address + %s;\n", model.valueLength());
        } else {
            throw new UnsupportedOperationException();
        }
        add("    }\n\n");
    }

    private void addPutField(FieldModel field, String prefix, String address, String value) {
        if (field.type().equals(Integer.TYPE)) {
            add("%sunsafe.putInt(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Long.TYPE)) {
            add("%sunsafe.putLong(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Short.TYPE)) {
            add("%sunsafe.putShort(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Float.TYPE)) {
            add("%sunsafe.putFloat(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Double.TYPE)) {
            add("%sunsafe.putDouble(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Boolean.TYPE)) {
            add("%sunsafe.putBoolean(null, %s, %s);", prefix, address, value);
        } else if (field.type().equals(Short.TYPE)) {
            add("unsafe.putShort(%s, %s);", prefix, address, value);
        } else if (field.type().equals(Character.TYPE)) {
            add("%sunsafe.putChar(%s, %s);", prefix, address, value);
        } else {
            throw new RuntimeException("Unhandled field");
        }
        add("\n");
    }

    private void generateReadValue() {
        add("    public %s readValue(long address){\n", model.valueClassName());
        add("        try{\n");
        add("            %s value = (%s)unsafe.allocateInstance(%s.class);\n", model.valueClassName(), model.valueClassName(), model.valueClassName());
        for (FieldModel field : model.valueFields()) {
            Class fieldType = field.type();
            String fromAddress = "address+" + (field.offset() - model.valueObjectHeaderSize());
            if (fieldType.equals(Integer.TYPE)) {
                add("            unsafe.putInt(value, %s, unsafe.getInt(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Long.TYPE)) {
                add("            unsafe.putLong(value, %s, unsafe.getLong(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Float.TYPE)) {
                add("            unsafe.putFloat(value, %s, unsafe.getFloat(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Double.TYPE)) {
                add("            unsafe.putDouble(value, %s, unsafe.getDouble(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Character.TYPE)) {
                add("            unsafe.putChar(value, %s, unsafe.getChar(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Short.TYPE)) {
                add("            unsafe.putShort(value, %s, unsafe.getShort(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Boolean.TYPE)) {
                add("            unsafe.putBoolean(value, %s, unsafe.getBoolean(null, %s));\n", field.offset(), fromAddress);
            } else if (fieldType.equals(Byte.TYPE)) {
                add("            unsafe.putByte(value, %s, unsafe.getByte(null, %s));\n", field.offset(), fromAddress);
            }
        }
        add("            return value;\n");
        add("        }catch(InstantiationException e){throw new RuntimeException(e);}\n");
        add("    }\n\n");
    }

    private void generateReadKey() {
        add("    public %s readKey(long entryAddress){\n", model.keyClassName());
        add("        return null;\n");
        add("    }\n\n");
    }

    private void generateKeyMatches() {
        add("    public boolean keyMatches(long entryAddress, %s key){\n", model.keyClassName());
        add("        return false;\n");
        add("    }\n\n");
    }
}
