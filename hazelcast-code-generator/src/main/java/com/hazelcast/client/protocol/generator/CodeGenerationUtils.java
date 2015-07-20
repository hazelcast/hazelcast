/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol.generator;

import javax.lang.model.element.TypeElement;

public final class CodeGenerationUtils {

    public static final int BYTE_BIT_COUNT = 8;
    public static final String CODEC_PACKAGE = "com.hazelcast.client.impl.protocol.codec.";
    public static final String DATA_FULL_NAME = "com.hazelcast.nio.serialization.Data";

    private CodeGenerationUtils() {
    }

    public static String capitalizeFirstLetter(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1);
    }


    public static String getPackageNameFromQualifiedName(String qualifiedClassName) {
        return qualifiedClassName.substring(0, qualifiedClassName.lastIndexOf("."));
    }

    public static String mergeIds(short classId, short methodId) {
        final String s = Integer.toHexString((classId << BYTE_BIT_COUNT) + methodId);
        return s.length() == 3 ? "0x0" + s : "0x" + s;
    }

    public static String getArrayType(String type) {
        int end = type.indexOf("[]");
        return type.substring(0, end).trim();
    }

    public static String getGenericType(String type) {
        int beg = type.indexOf("<");
        int end = type.lastIndexOf(">");
        return type.substring(beg + 1, end).trim();
    }

    public static String getFirstGenericParameterType(String type) {
        int beg = type.indexOf("<");
        int end = type.lastIndexOf(",");
        return type.substring(beg + 1, end).trim();
    }

    public static String getSecondGenericParameterType(String type) {
        int beg = type.indexOf(",");
        int end = type.lastIndexOf(">");
        return type.substring(beg + 1, end).trim();
    }

    public static boolean isPrimitive(String type) {
        return type.equals("int") || type.equals("long") || type.equals("short") || type.equals("byte") || type
                .equals("boolean");
    }

    public static String getTypeCategory(String type) {
        int endIndex = type.indexOf('<');
        String t = endIndex > 0 ? type.substring(0, endIndex) : type;
        if (CodecModel.CUSTOM_CODEC_MAP.containsKey(t)) {
            return "CUSTOM";
        } else if (t.equals("java.util.Map")) {
            return "MAP";
        } else if (t.equals("java.util.List") || t.equals("java.util.Set") || t.equals("java.util.Collection")) {
            return "COLLECTION";
        } else if (type.endsWith("[]")) {
            return "ARRAY";
        }
        return "OTHER";
    }

    public static String getTypeCodec(String type) {
        int endIndex = type.indexOf('<');
        String t = endIndex > 0 ? type.substring(0, endIndex) : type;
        TypeElement typeElement = CodecModel.CUSTOM_CODEC_MAP.get(t);
        return typeElement != null ? typeElement.toString() : "";
    }


    public static String convertTypeToCSharp(String type) {
        String getterString;
        if (type.equals(DATA_FULL_NAME + " ")) {
            getterString = "IData";
        } else if (type.equals("java.lang.String")) {
            getterString = "string";
        } else if (type.equals("boolean")) {
            getterString = "bool";
        } else if (type.equals("java.util.List<" + DATA_FULL_NAME + " >")) {
            getterString = "IList<IData>";
        } else if (type.equals("java.util.Set<" + DATA_FULL_NAME + " >")) {
            getterString = "ISet<IData>";
        } else if (type.equals("java.util.Collection<" + DATA_FULL_NAME + " >")) {
            getterString = "ICollection<IData>";
        } else {
            getterString = type;
        }
        return getterString;
    }


}
