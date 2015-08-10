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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class CodeGenerationUtils {

    public static final int BYTE_BIT_COUNT = 8;
    public static final String CODEC_PACKAGE = "com.hazelcast.client.impl.protocol.codec.";
    public static final String DATA_FULL_NAME = "com.hazelcast.nio.serialization.Data";


    private static final Map<String, String> JAVA_TO_CSHARP_TYPES = new HashMap<String, String>() {{
        put(DATA_FULL_NAME, "IData");
        put("java.lang.String", "string");
        put("java.lang.Integer", "int");
        put("boolean", "bool");
        put("java.util.List", "IList");
        put("java.util.Set", "ISet");
        put("java.util.Collection", "ICollection");
        put("java.util.Map", "IDictionary");
        put("java.util.Map.Entry", "KeyValuePair");
        put("com.hazelcast.nio.Address", "Address");
        put("com.hazelcast.client.impl.client.DistributedObjectInfo", "DistributedObjectInfo");
        put("com.hazelcast.core.Member", "Core.IMember");
        put("com.hazelcast.cluster.client.MemberAttributeChange", "Hazelcast.Client.Request.Cluster.MemberAttributeChange");
        put("com.hazelcast.map.impl.SimpleEntryView", "Hazelcast.Map.SimpleEntryView");
    }};

    private CodeGenerationUtils() {
    }

    public static String capitalizeFirstLetter(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1);
    }


    public static String getPackageNameFromQualifiedName(String qualifiedClassName) {
        return qualifiedClassName.substring(0, qualifiedClassName.lastIndexOf("."));
    }

    public static String mergeIds(short classId, short methodId) {
        return Integer.toHexString((classId << BYTE_BIT_COUNT) + methodId);
    }

    public static String addHexPrefix(String s) {
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

    public static String getSimpleType(String type) {
        int beg = type.indexOf("<");
        return type.substring(0, beg);
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

    public static boolean isGeneric(String type) {
        return type.contains("<");
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

    public static String getConvertedType(String type) {
        if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<") || type.startsWith("java.util.Collection<")) {
            return type.replaceAll("java.util.*<(.*)>", "java.util.Collection<$1>");
        }
        return type;
    }
    
    public static String getDescription(String parameterName, String commentString) {
        String result  = "";
        if (null != parameterName && null != commentString) {
            int start = commentString.indexOf("@param");
            if (start >= 0) {
                String paramString = commentString.substring(start);
                String[] paramStrings = paramString.split("@param");
                for (String parameterString : paramStrings) {
                    /**
                     * Example such string is
                     * key      key of the entry
                     */

                    String trimmedParameterString = parameterString.trim();
                    if (trimmedParameterString.length() > parameterName.length() && trimmedParameterString.startsWith(parameterName)) {
                        result = trimmedParameterString.substring(parameterName.length());
                        int endIndex = result.indexOf('@');
                        if (endIndex >= 0) {
                            result = result.substring(0, endIndex);
                        }

                        // replace any new line with <br>
                        result = result.replace("\n", "<br>");
                        result = result.trim();

                        break; // found the parameter, hence stop here
                    }
                }
            }
        }
        return result;
    }

    public static String getReturnDescription(String commentString) {
        String result  = "";
        final String RETURN_TAG = "@return";
        int returnTagStartIndex = commentString.indexOf(RETURN_TAG);
        if (returnTagStartIndex >= 0) {
            int descriptionStartIndex = returnTagStartIndex + RETURN_TAG.length();
            int nextTagIndex = commentString.indexOf("@", descriptionStartIndex);
            if (nextTagIndex >= 0) {
                result = commentString.substring(descriptionStartIndex, nextTagIndex);
            } else {
                result = commentString.substring(descriptionStartIndex);
            }
            result = result.trim();

            // replace any new line with <br>
            result = result.replace("\n", "<br>");
        }
        return result;
    }

    public static String getDistributedObjectName(String templateClassName) {
        String result = templateClassName;

        int startIndex = templateClassName.lastIndexOf('.');
        if (startIndex >= 0) {
            int endIndex = templateClassName.indexOf("CodecTemplate", startIndex);
            if (endIndex > startIndex) {
                result = templateClassName.substring(startIndex + 1, endIndex);
            }
        }

        return result;
    }

    // parse generic type parameters, making sure nested generics are taken into account
    private static List<String> getGenericTypeParameters(String parameters) {
        List<String> paramList = new ArrayList<String>();
        int balanced = 0;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < parameters.length(); i++) {
            char c = parameters.charAt(i);
            if (balanced == 0 && c == ',') {
                paramList.add(current.toString().trim());
                current = new StringBuilder();
                continue;
            }
            else if (c == '<') {
                balanced++;
            } else if (c == '>') {
                balanced--;
            }
            current.append(c);
        }
        paramList.add(current.toString());
        return paramList;
    }

    public static String getCSharpType(String type) {
        type = type.trim();
        if (isGeneric(type)) {
            String simpleType = getCSharpType(getSimpleType(type));
            String genericParameters = getGenericType(type);

            List<String> typeParameters = getGenericTypeParameters(genericParameters);
            StringBuilder builder = new StringBuilder();

            builder.append(simpleType).append('<');

            Iterator<String> iterator = typeParameters.iterator();
            while (iterator.hasNext()) {
                builder.append(getCSharpType(iterator.next()));
                if (iterator.hasNext()) {
                    builder.append(',');
                }
            }
            builder.append(">");
            return builder.toString();
        }

        String convertedType = JAVA_TO_CSHARP_TYPES.get(type);

        return convertedType == null ? type : convertedType;
    }

}
