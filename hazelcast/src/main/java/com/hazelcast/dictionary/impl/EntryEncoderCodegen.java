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

import com.hazelcast.dictionary.impl.type.ClassField;
import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.dictionary.impl.type.Type;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.util.StringUtil.uppercaseFirstLetter;

public class EntryEncoderCodegen extends AbstractCodeGen {
    private final EntryType entryType;
    private final Type valueType;
    private final Type keyType;

    public EntryEncoderCodegen(EntryType entryType) {
        this.entryType = entryType;
        this.valueType = entryType.value();
        this.keyType = entryType.key();
    }

    public String className() {
        return entryType.dictionaryName() + "_Encoder";
    }

    public void generate() {
        addLn("import java.util.*;");
        addLn("import com.hazelcast.util.*;");
        addLn("import com.hazelcast.dictionary.impl.*;");
        addLn("import com.hazelcast.dictionary.impl.type.*;");
        addLn("import static com.hazelcast.nio.Bits.*;");
        addLn();
        addLn();
        addLn("public final class %s", className());
        addLn("             extends com.hazelcast.dictionary.impl.EntryEncoder<%s, %s>{", keyType.name(), valueType.name());
        addLn();
        indent();
        generateConstructor();
        addLn();
        generateWriteEntry();
        addLn();
        generateWriteKey();
        addLn();
        generateWriteValue();
        addLn();
        generateReadValue();
        addLn();
        generateReadKey();
        addLn();
        generateKeyMatches();
        addLn();
        generateSize();
        unindent();
        addLn("}");
    }


    private void generateConstructor() {
        addLn("public %s(EntryType entryType){", className());
        indent();
        addLn("super(entryType);");
        unindent();
        addLn("}");
    }

    private void generateWriteEntry() {
        addLn("@Override");
        addLn("public int writeEntry(final %s key, final %s value, long address, int available){",
                keyType.name(), valueType.name());
        indent();
        addLn("// check if there is enough space for the fixed length part");

        // todo: assumes key is fixed length
        addLn("if (available < %s) return -1;", keyType.fixedLength() + valueType.fixedLength());
        addLn();
        key_writeFixedLengthFields();
        addLn("int position=%s;", keyType.fixedLength());

        switch (valueType.kind()) {
            case FIXED_LENGTH_RECORD:
                value_writeFixedLengthFields();
                addLn("position+=%s;", valueType.fixedLength());
                break;
            case VARIABLE_LENGTH_RECORD:
                value_writeFixedLengthFields();
                addLn("position+=%s;", valueType.fixedLength());
                value_writeVariableLengthFields();
                break;
            case STRING:
                throw new RuntimeException();
            case PRIMITIVE_ARRAY:
                addLn("position+=unsafeSupport.putArray_%s(address+position, value);",
                        valueType.arrayElementClass().getName());
                break;
            default:
                throw new IllegalStateException("Unhandled value type:" + valueType);
        }

        addLn("return position;");
        unindent();
        addLn("}");
    }

    private void generateWriteKey() {
        addLn("@Override");
        addLn("public long writeKey(%s key, long address){", keyType.name());
        indent();
        //if (entryType.isFixedLengthKey()) {
        key_writeFixedLengthFields();
        addLn("return address + %s;", keyType.fixedLength());
        //} else {
        //    throw new UnsupportedOperationException();
        // }
        unindent();
        addLn("}");
    }

    private void key_writeFixedLengthFields() {
        if (keyType.fixedLengthFields().isEmpty()) {
            return;
        }

        addLn("// Writing key: fixed length fields");

        for (ClassField field : keyType.fixedLengthFields()) {
            switch (field.kind()) {
                case PRIMITIVE:
                    addLn("unsafe.%s(null, address+%s, unsafe.%s(key, %s));",
                            unsafePutMethod(field.clazz()), field.offsetOffheap(),
                            unsafeGetMethod(field.clazz()), field.offsetHeap());
                    break;
                case PRIMITIVE_WRAPPER:
                    addLn("unsafeSupport.put_%s(address, key, %s, %s);",
                            field.clazz().getSimpleName(), field.offsetOffheap(), field.offsetHeap());
                    break;
                default:
                    throw new IllegalStateException("Unhandled field:" + field);
            }
        }
        addLn();
    }

    private void value_writeFixedLengthFields() {
        addLn("// Writing value: fixed length fields");

        for (ClassField field : valueType.fixedLengthFields()) {
            switch (field.kind()) {
                case PRIMITIVE:
                    addLn("unsafe.%s(null, address+position+%s, unsafe.%s(value, %s));",
                            unsafePutMethod(field.clazz()), field.offsetOffheap(),
                            unsafeGetMethod(field.clazz()), field.offsetHeap());
                    break;
                case PRIMITIVE_WRAPPER:
                    addLn("unsafeSupport.put_%s(address+position+%s, value, %s);",
                            field.clazz().getSimpleName(), field.offsetOffheap(), field.offsetHeap());
                    break;
                default:
                    throw new IllegalStateException("Unhandled field:" + field);
            }
        }
        addLn();
    }

    private void value_writeVariableLengthFields() {
        addLn("// writing the variable length fields");

        for (ClassField field : valueType.variableLengthFields()) {
            switch (field.kind()) {
                case PRIMITIVE_ARRAY:
                    Class arrayElementType = field.arrayElementType();
                    addLn("position += unsafeSupport.putArray_%s(address + position, value, %s);",
                            arrayElementType.getName(), field.offsetHeap());
                    break;
                default:
                    throw new IllegalStateException("Unhandled field:" + field);
            }
        }

        addLn();
    }

    private void generateWriteValue() {
        addLn("@Override");
        addLn("public long writeValue(final %s value, final long address){", valueType.name());
        indent();

        if (!(keyType.isFixedLength() && valueType.isFixedLength())) {
            addLn("if(true)throw new RuntimeException();");
        } else {
            // throw new UnsupportedOperationException();
        }

        switch (valueType.kind()) {
            case FIXED_LENGTH_RECORD:
                addLn("int position=%s;", keyType.fixedLength());
                value_writeFixedLengthFields();
                break;
            case VARIABLE_LENGTH_RECORD:
                break;
            case PRIMITIVE_ARRAY:
                break;
            default:
                throw new RuntimeException();
        }

        add("        return 0;\n");

        unindent();
        addLn("}");
    }

    private void generateReadValue() {
        addLn("@Override");
        addLn("public %s readValue(final long address){", valueType.name());
        indent();

        addLn("int position = %s;", keyType.fixedLength());

        switch (valueType.kind()) {
            case FIXED_LENGTH_RECORD:
                newRecordInstance();
                readFixedLengthFields();
                addLn("return value;");
                break;
            case VARIABLE_LENGTH_RECORD:
                newRecordInstance();
                readFixedLengthFields();
                readVariableLengthFields();
                addLn("return value;");
                break;
            case PRIMITIVE_ARRAY:
                String elementType = valueType.arrayElementClass().getName();
                addLn("int length = unsafe.getInt(null, address+position);");
                addLn("if (length==-1) return null;");
                addLn();
                addLn("position+=INT_SIZE_IN_BYTES;");
                addLn("%s[] value = new %s[length];", elementType, elementType);
                addLn("long bytes = length*unsafeSupport.arrayIndexScale_%s;", elementType);
                addLn("unsafe.copyMemory(null, address+position, value, unsafeSupport.arrayBaseOffset_%s, bytes);", elementType);
                addLn("return value;");
                break;
            default:
                throw new IllegalStateException("Unhandled valueType:" + valueType);
        }

        unindent();
        addLn("}");
    }

    private void newRecordInstance() {
        addLn("%s value;", valueType.name());
        addLn("try{");
        // todo: this can be optimized if there is a public constructor? Do we want that?
        addLn("    value = (%s)unsafe.allocateInstance(%s.class);", valueType.name(), valueType.name());
        addLn("}catch(InstantiationException e){throw new RuntimeException(e);}");
        addLn();
    }

    private void readVariableLengthFields() {
        if (valueType.variableLengthFields().isEmpty()) {
            return;
        }

        addLn("// ===========================================");
        addLn("// reading the variable length fields");
        addLn("// ===========================================");
        addLn();

        for (ClassField field : valueType.variableLengthFields()) {
            Class fieldType = field.clazz();
            switch (field.kind()) {
                case PRIMITIVE_ARRAY:
                    if (byte[].class.equals(fieldType)) {
                        addLn("int length=unsafe.getInt(null, address+position);");
                        addLn("position+=INT_SIZE_IN_BYTES;");
                        addLn("if(length==-1){");
                        indent();
                        addLn("// ignore");
                        unindent();
                        addLn("} else {");
                        indent();
                        addLn("byte[] bytes = new byte[length];");
                        addLn("unsafe.copyMemory(null,address+position, "
                                + "bytes, unsafe.arrayBaseOffset(byte[].class), length);\n");
                        addLn("unsafe.putObject(value, %s, bytes);", field.offsetHeap());
                        addLn("position += length;");
                        unindent();
                        addLn("}");
                        addLn();
                    } else {
//                            throw new RuntimeException();
                    }
                    break;
                default:
                    throw new IllegalStateException("Unhandled field:" + field);
            }
        }
    }

    private void readFixedLengthFields() {
        if (!valueType.fixedLengthFields().isEmpty()) {
            addLn("// ===========================================");
            addLn("// reading the fixed length fields");
            addLn("// ===========================================");

            for (ClassField field : valueType.fixedLengthFields()) {
                switch (field.kind()) {
                    case PRIMITIVE:
                        addLn("unsafe.%s(value, %s, unsafe.%s(null, address+position+%s));",
                                unsafePutMethod(field.clazz()), field.offsetHeap(),
                                unsafeGetMethod(field.clazz()), field.offsetOffheap());
                        break;
                    case PRIMITIVE_WRAPPER:
                        Class primitiveType = extractPrimitiveType(field.clazz());
                        addLn("if (unsafe.getBoolean(null, address+position+ %s)) {", field.offsetOffheap());
                        indent();
                        addLn("unsafe.putObject(value, %s, unsafe.%s(null, address+position+%s));",
                                field.offsetHeap(), unsafeGetMethod(primitiveType),
                                field.offsetOffheap() + BOOLEAN_SIZE_IN_BYTES);
                        unindent();
                        addLn("}");
                        break;
                    default:
                        throw new IllegalStateException("Unhandled field:" + field);
                }
            }
            addLn();
        }
    }

    private static String unsafeGetMethod(Class type) {
        return "get" + uppercaseFirstLetter(type.getName());
    }

    private static String unsafePutMethod(Class type) {
        return "put" + uppercaseFirstLetter(type.getName());
    }

    private static Class extractPrimitiveType(Class type) {
        if (Boolean.class.equals(type)) {
            return boolean.class;
        } else if (Byte.class.equals(type)) {
            return byte.class;
        } else if (Character.class.equals(type)) {
            return char.class;
        } else if (Short.class.equals(type)) {
            return short.class;
        } else if (Integer.class.equals(type)) {
            return int.class;
        } else if (Long.class.equals(type)) {
            return long.class;
        } else if (Float.class.equals(type)) {
            return float.class;
        } else if (Double.class.equals(type)) {
            return double.class;
        } else {
            throw new RuntimeException("type [" + type + "] is not a primitive type");
        }
    }

    private void generateReadKey() {
        addLn("@Override");
        addLn("public %s readKey(final long entryAddress){", keyType.name());
        indent();
        addLn("return null;");
        unindent();
        addLn("}");
    }

    private void generateKeyMatches() {
        addLn("@Override");
        addLn("public boolean keyMatches(final long address, %s key){", keyType.name());
        indent();

        switch (keyType.kind()) {
            case FIXED_LENGTH_RECORD:
                for (ClassField field : keyType.fixedLengthFields()) {
                    switch (field.kind()) {
                        case PRIMITIVE:
                            String getMethod = unsafeGetMethod(field.clazz());
                            addLn("if(unsafe.%s(null, address+%s)!=unsafe.%s(key,%s)) return false;",
                                    getMethod, field.offsetOffheap(), getMethod, field.offsetHeap());
                            break;
                        default:
                            throw new IllegalStateException("Unhandled field:" + field);
                    }
                }
                addLn("return true;");
                break;
            default:
                throw new IllegalStateException("Unhandled valueType:" + valueType);
        }
        unindent();
        addLn("}");
    }

    private void generateSize() {
        addLn("@Override");
        addLn("public int size(final long address){", className());
        indent();
        if (keyType.isFixedLength() && valueType.isFixedLength()) {
            addLn("return %s;", keyType.fixedLength() + valueType.fixedLength());
        }else{
            addLn("return unsafe.getInt(null, address);");
        }
        unindent();
        addLn("}");
    }
}
