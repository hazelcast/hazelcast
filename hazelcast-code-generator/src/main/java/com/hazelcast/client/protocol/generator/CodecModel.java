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

import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CodecModel {

    private static final String PARAMETERS_PACKAGE = "com.hazelcast.client.impl.protocol.codec.";
    private static final String DATA_FULL_NAME = "com.hazelcast.nio.serialization.Data";

    private final Lang lang;
    private String name;
    private String className;
    private String parentName;
    private String packageName;

    private int retryable;
    private int response;

    private final List<ParameterModel> requestParams = new LinkedList();
    private final List<ParameterModel> responseParams = new LinkedList();
    private final List<EventModel> events = new LinkedList();

    public CodecModel(TypeElement parent, ExecutableElement methodElement, ExecutableElement responseElement,
                      List<ExecutableElement> eventElementList, boolean retryable, Lang lang) {
        this.retryable = retryable ? 1 : 0;
        this.lang = lang;

        name = methodElement.getSimpleName().toString();
        parentName = parent.getAnnotation(GenerateCodec.class).name();
        className = CodeGenerationUtils.capitalizeFirstLetter(parentName)
                + CodeGenerationUtils.capitalizeFirstLetter(name) + "Codec";
        packageName = "com.hazelcast.client.impl.protocol.codec";

        //        if (lang != Lang.JAVA) {
        //            packageName = classElement.getAnnotation(GenerateParameters.class).ns();
        //        }

        response = methodElement.getAnnotation(Request.class).response();
        initParameters(methodElement, responseElement, eventElementList, lang);

    }

    private void initParameters(ExecutableElement methodElement, ExecutableElement responseElement,
                                List<ExecutableElement> eventElementList, Lang lang) {
        //request parameters
        for (VariableElement param : methodElement.getParameters()) {
            final Nullable nullable = param.getAnnotation(Nullable.class);

            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            pm.lang = lang;
            pm.isPrimitive = param.asType().getKind().isPrimitive();
            pm.isNullable = nullable != null;
            requestParams.add(pm);
        }

        //response parameters
        for (VariableElement param : responseElement.getParameters()) {
            final Nullable nullable = param.getAnnotation(Nullable.class);
            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            pm.lang = lang;
            pm.isPrimitive = param.asType().getKind().isPrimitive();
            pm.isNullable = nullable != null;
            responseParams.add(pm);
        }


        //event parameters
        for (ExecutableElement element : eventElementList) {
            List<ParameterModel> eventParam = new ArrayList<ParameterModel>();
            for (VariableElement param : element.getParameters()) {
                final Nullable nullable = param.getAnnotation(Nullable.class);
                ParameterModel pm = new ParameterModel();
                pm.name = param.getSimpleName().toString();
                pm.type = param.asType().toString();
                pm.lang = lang;
                pm.isPrimitive = param.asType().getKind().isPrimitive();
                pm.isNullable = nullable != null;
                eventParam.add(pm);
            }

            EventModel eventModel = new EventModel();
            eventModel.type = element.getAnnotation(EventResponse.class).value();
            eventModel.name = element.getSimpleName().toString();
            eventModel.eventParams = eventParam;

            events.add(eventModel);
        }
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getParentName() {
        return parentName;
    }

    public String getPackageName() {
        return packageName;
    }

    public int getResponse() {
        return response;
    }

    public List<ParameterModel> getRequestParams() {
        return requestParams;
    }

    public List<ParameterModel> getResponseParams() {
        return responseParams;
    }

    public List<EventModel> getEvents() {
        return events;
    }

    public int getRetryable() {
        return retryable;
    }

    public static class EventModel {
        private String name;
        private List<ParameterModel> eventParams;
        private int type;

        public int getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public String getTypeString() {
            return "EVENT_" + name.toUpperCase();
        }

        public List<ParameterModel> getEventParams() {
            return eventParams;
        }
    }

    public static class ParameterModel {
        private String name;
        private String type;
        private Lang lang;
        private boolean isNullable;
        private boolean isPrimitive;

        public String getName() {
            return name;
        }

        public String getType() {
            if (lang == Lang.CSHARP) {
                return convertTypeToCSharp(type);
            }
            if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<")
                    || type.startsWith("java.util.Collection<")) {
                return type.replaceAll("java.util.*<(.*)>", "java.util.Collection<$1>");
            }
            return type;
        }

        public String getSizeString() {
            if (lang == Lang.CSHARP) {
                return getSizeStringCSharp();
            }
            return getSizeStringJava();
        }

        public String getSizeStringJava() {
            String stringJava = resolveSizeStringJava(type, name);
            return getNullableCheckedSizeStringJava(stringJava);
        }

        private String getNullableCheckedSizeStringJava(String innerString) {
            StringBuilder sizeString = new StringBuilder();
            if (isNullable) {
                sizeString.append("dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;\n");
                sizeString.append("        if (" + name + " != null) {\n");
                sizeString.append(innerString);
                sizeString.append("        }\n");
                return sizeString.toString();
            } else {
                return innerString;
            }
        }

        private String resolveSizeStringJava(String type, String name) {
            StringBuilder sizeString = new StringBuilder();
            if (type.equals(DATA_FULL_NAME)) {
                sizeString.append("dataSize += ParameterUtil.calculateDataSize(" + name + ");");
            } else if (type.equals("java.lang.Integer")) {
                sizeString.append("dataSize += Bits.INT_SIZE_IN_BYTES;");
            } else if (type.equals("java.lang.Boolean")) {
                sizeString.append("dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;");
            } else if (type.equals("java.lang.String")) {
                sizeString.append("dataSize += ParameterUtil.calculateStringDataSize(" + name + ");");
            } else if (type.equals("int") || type.equals("long") || type.equals("short")
                    || type.equals("byte") || type.equals("boolean")) {
                sizeString.append("dataSize += Bits." + type.toUpperCase() + "_SIZE_IN_BYTES;");
            } else if (type.equals("com.hazelcast.nio.Address")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "AddressCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.core.Member")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "MemberCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.cluster.client.MemberAttributeChange")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE
                        + "MemberAttributeChangeCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.map.impl.SimpleEntryView<" + DATA_FULL_NAME
                    + "," + DATA_FULL_NAME + ">")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "EntryViewCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.client.impl.client.DistributedObjectInfo")) {
                sizeString.append("dataSize += "
                        + PARAMETERS_PACKAGE + "DistributedObjectInfoCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.mapreduce.JobPartitionState")) {
                sizeString.append("dataSize += "
                        + PARAMETERS_PACKAGE + "JobPartitionStateCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("javax.transaction.xa.Xid")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "XIDCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.map.impl.querycache.event.SingleEventData")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "SingleEventDataCodec.calculateDataSize(" + name + ");");
            } else if (type.equals("com.hazelcast.cache.impl.CacheEventData")) {
                sizeString.append("dataSize += " + PARAMETERS_PACKAGE + "CacheEventDataCodec.calculateDataSize(" + name + ");");
            } else if (type.startsWith("java.util.Map<")) {
                sizeString.append(getMapSizeStringJava(type, name));
            } else if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<")
                    || type.startsWith("java.util.Collection<")) {

                sizeString.append(getCollectionSizeString(name, type));

            } else if (type.endsWith("[]")) {
                sizeString.append(getArraySizeString(type, name));
            } else {
                sizeString.append(CodeGenerationUtils.capitalizeFirstLetter(type) + "Codec.calculateDataSize(" + name + ");");
            }
            return sizeString.toString();
        }

        private String getArraySizeString(String type, String name) {
            String itemType = CodeGenerationUtils.getTypeInsideData(type);
            StringBuilder builder = new StringBuilder();
            builder.append("dataSize += Bits.INT_SIZE_IN_BYTES;\n        ");
            builder.append("for (" + itemType + " " + name + "_item : " + name + " ) {\n        ");
            builder.append("    " + resolveSizeStringJava(itemType, name + "_item") + "\n        ");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getCollectionSizeString(String name, String type) {
            String subType = CodeGenerationUtils.getTypeInsideCollection(type);
            StringBuilder builder = new StringBuilder();
            builder.append("dataSize += Bits.INT_SIZE_IN_BYTES;\n        ");
            builder.append("for (" + subType + " " + name + "_item : " + name + " ) {\n        ");
            builder.append("    " + resolveSizeStringJava(subType, name + "_item") + "\n        ");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getMapSizeStringJava(String type, String name) {
            StringBuilder builder = new StringBuilder();
            String keyType = CodeGenerationUtils.getKeyTypeInsideMap(type);
            builder.append("java.util.Collection<" + keyType + "> " + name
                    + "_keySet = (java.util.Collection<" + keyType + ">) " + name + ".keySet();\n     ");
            builder.append(resolveSizeStringJava("java.util.Collection<" + keyType + "> ", name + "_keySet"));

            String valueType = CodeGenerationUtils.getValueTypeInsideMap(type);
            builder.append("java.util.Collection<" + valueType + "> " + name
                    + "_values = (java.util.Collection<" + valueType + "> )" + name + ".values();\n       ");
            builder.append(resolveSizeStringJava("java.util.Collection<" + valueType + ">", name + "_values"));
            return builder.toString();
        }


        public String getSizeStringCSharp() {
            if (type.equals(DATA_FULL_NAME)) {
                return "ParameterUtil.CalculateDataSize(" + name + ")";
            } else if (type.equals("java.lang.String")) {
                return "ParameterUtil.CalculateStringDataSize(" + name + ")";
            } else if (type.equals("byte[]")) {
                return "ParameterUtil.CalculateByteArrayDataSize(" + name + ")";
            } else if (type.equals("java.util.List<" + DATA_FULL_NAME + " >")
                    || type.equals("java.util.Set<" + DATA_FULL_NAME + " >")
                    || type.equals("java.util.Collection<" + DATA_FULL_NAME + " >")) {
                return "ParameterUtil.CalculateCollectionDataSize(" + name + ")";
            }
            return "BitUtil.SizeOf" + CodeGenerationUtils.capitalizeFirstLetter(type);
        }

        public String getDataGetterString() {
            if (lang == Lang.CSHARP) {
                return getDataGetterStringCSharp();
            }
            return getDataGetterStringJava();
        }

        public String getEventGetterString() {
            String getterString = resolveDataGetterStringJava(type, name) + "\n        ";
            return getNullableCheckedGetterStringJava(getterString);
        }

        public String getDataGetterStringJava() {
            String getterString = resolveDataGetterStringJava(type, name) + "\n        ";
            getterString += "    parameters." + name + " = " + name + "; \n        ";
            return getNullableCheckedGetterStringJava(getterString);
        }

        private String getNullableCheckedGetterStringJava(String innerGetterString) {
            String getterString = type + " " + name + ";\n        ";
            if (!isPrimitive) {
                getterString += name + " = null ;\n        ";
            }

            if (isNullable) {
                getterString += " boolean " + name + "_isNull = clientMessage.getBoolean();\n        ";
                getterString += " if(!" + name + "_isNull) { \n        ";
            }

            getterString += innerGetterString;

            if (isNullable) {
                getterString += "\n             }\n        ";
            }
            return getterString;
        }

        private String resolveDataGetterStringJava(String type, String name) {
            String getterString;

            if (type.equals(DATA_FULL_NAME)) {
                getterString = name + " = clientMessage.getData();";
            } else if (type.equals("java.lang.Integer")) {
                getterString = name + " = clientMessage.getInt();";
            } else if (type.equals("java.lang.Boolean")) {
                getterString = name + " = clientMessage.getBoolean();";
            } else if (type.equals("java.lang.String")) {
                getterString = name + " = clientMessage.getStringUtf8();";
            } else if (type.equals("com.hazelcast.nio.Address")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "AddressCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.core.Member")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "MemberCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.cluster.client.MemberAttributeChange")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "MemberAttributeChangeCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.map.impl.SimpleEntryView<" + DATA_FULL_NAME
                    + "," + DATA_FULL_NAME + ">")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "EntryViewCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.client.impl.client.DistributedObjectInfo")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "DistributedObjectInfoCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.mapreduce.JobPartitionState")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "JobPartitionStateCodec.decode(clientMessage);";
            } else if (type.equals("javax.transaction.xa.Xid")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "XIDCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.cache.impl.CacheEventData")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "CacheEventDataCodec.decode(clientMessage);";
            } else if (type.equals("com.hazelcast.map.impl.querycache.event.SingleEventData")) {
                getterString = name + " = " + PARAMETERS_PACKAGE + "SingleEventDataCodec.decode(clientMessage);";
            } else if (type.startsWith("java.util.Map<")) {
                getterString = getMapGetterString(type, name);
            } else if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<")
                    || type.startsWith("java.util.Collection<")) {
                getterString = getCollectionGetterString(type, name);
            } else if (type.endsWith("[]")) {
                getterString = getArrayGetterString(type, name);
            } else {
                getterString = name + " = clientMessage.get" + CodeGenerationUtils.capitalizeFirstLetter(type) + "();";
            }


            return getterString;
        }

        private String getArrayGetterString(String type, String name) {
            String itemVariableType = CodeGenerationUtils.getTypeInsideData(type);
            String itemVariableName = name + "_item";

            String sizeVariableName = name + "_size";
            String indexVariableName = name + "_index";

            StringBuilder builder = new StringBuilder();
            builder.append("int " + sizeVariableName + " = clientMessage.getInt();\n        ");
            builder.append(name + " = new " + itemVariableType + "[" + sizeVariableName + "];\n        ");
            builder.append("for (int " + indexVariableName + " = 0; " + indexVariableName + " < "
                    + sizeVariableName + "; " + indexVariableName + "++) {\n        ");
            builder.append("    " + itemVariableType + " "
                    + resolveDataGetterStringJava(itemVariableType, itemVariableName) + "\n        ");
            builder.append("    " + name + "[" + indexVariableName + "] = " + itemVariableName + ";");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getCollectionGetterString(String type, String name) {
            String itemVariableType = CodeGenerationUtils.getTypeInsideCollection(type);
            String itemVariableName = name + "_item";

            StringBuilder builder = new StringBuilder();
            String sizeVariableName = name + "_size";
            String indexVariableName = name + "_index";

            builder.append("int " + sizeVariableName + " = clientMessage.getInt();\n        ");
            String collectionType = getCollectionType(type);
            builder.append(name + " = new " + collectionType + "<" + itemVariableType + ">("
                    + sizeVariableName + ");\n        ");
            builder.append("for (int " + indexVariableName + " = 0; " + indexVariableName + " < "
                    + sizeVariableName + "; " + indexVariableName + "++) {\n        ");
            builder.append("    " + itemVariableType + " " + itemVariableName + ";\n        ");
            builder.append("    " + resolveDataGetterStringJava(itemVariableType, itemVariableName) + "\n        ");
            builder.append("    " + name + ".add( " + itemVariableName + ");\n      ");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getMapGetterString(String type, String name) {
            String keyType = CodeGenerationUtils.getKeyTypeInsideMap(type);

            StringBuilder builder = new StringBuilder();
            builder.append("java.util.List<" + keyType + "> " + name + "_keySet;\n     ");
            builder.append(resolveDataGetterStringJava("java.util.List<" + keyType + "> ", name + "_keySet"));

            String valueType = CodeGenerationUtils.getValueTypeInsideMap(type);
            builder.append("java.util.List<" + valueType + "> " + name + "_values;\n       ");
            builder.append(resolveDataGetterStringJava("java.util.List<" + valueType + ">", name + "_values"));


            String mapIndexVariableName = name + "_index";
            builder.append(name + " = new java.util.HashMap<" + keyType + "," + valueType + ">();\n       ");

            builder.append("for (int " + mapIndexVariableName + " = 0; " + mapIndexVariableName + " < "
                    + name + "_keySet_size; " + mapIndexVariableName + "++) {\n        ");
            builder.append("    " + name + ".put( " + name + "_keySet.get(" + mapIndexVariableName + ") , "
                    + name + "_values.get(" + mapIndexVariableName + ") );\n        ");
            builder.append("}\n        ");

            return builder.toString();
        }

        private String getCollectionType(String name) {
            if (name.startsWith("java.util.Set")) {
                return "java.util.HashSet";
            } else {
                return "java.util.ArrayList";
            }
        }

        public String getDataGetterStringCSharp() {
            String getterString;
            if (type.equals(DATA_FULL_NAME + " ")) {
                getterString = "GetData";
            } else if (type.equals("java.lang.String")) {
                getterString = "GetStringUtf8";
            } else if (type.equals("byte[]")) {
                getterString = "GetByteArray";
            } else if (type.equals("java.util.List<" + DATA_FULL_NAME + " >")) {
                getterString = "GetDataList";
            } else if (type.equals("java.util.Set<" + DATA_FULL_NAME + " >")) {
                getterString = "GetDataSet";
            } else if (type.equals("java.util.Collection<" + DATA_FULL_NAME + " >")) {
                getterString = "GetDataSet";
            } else {
                getterString = "Get" + CodeGenerationUtils.capitalizeFirstLetter(type);
            }
            return getterString;
        }


        public String getDataSetterString() {
//            if (lang == Lang.CSHARP) {
//                return getDataSetterStringCSharp();
//            }
            return getDataSetterStringJava();
        }

        public String getDataSetterStringJava() {
            String setterString = resolveDataSetterStringJava(type, name);
            return getNullableCheckedSetterStringJava(setterString);
        }

        private String getNullableCheckedSetterStringJava(String innerGetterString) {
            StringBuilder setterString = new StringBuilder();


            String isNullVariableName = name + "_isNull";
            if (isNullable) {
                setterString.append("boolean " + isNullVariableName + ";\n            ");
                setterString.append("if (" + name + " == null) {\n            ");
                setterString.append("    " + isNullVariableName + " = true;\n            ");
                setterString.append("    clientMessage.set(" + isNullVariableName + ");\n            ");
                setterString.append("} else {\n            ");
                setterString.append("" + isNullVariableName + " = false;\n            ");
                setterString.append("clientMessage.set(" + isNullVariableName + ");\n            ");
                setterString.append(innerGetterString);
                setterString.append("} \n            ");

                return setterString.toString();
            } else {
                return innerGetterString;
            }

        }

        private String resolveDataSetterStringJava(String type, String name) {
            StringBuilder setterString = new StringBuilder();
            if (type.equals("com.hazelcast.nio.Address")) {
                setterString.append(PARAMETERS_PACKAGE + "AddressCodec.encode(" + name + ",clientMessage);");
            } else if (type.equals("com.hazelcast.core.Member")) {
                setterString.append(PARAMETERS_PACKAGE + "MemberCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.cluster.client.MemberAttributeChange")) {
                setterString.append(PARAMETERS_PACKAGE
                        + "MemberAttributeChangeCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.map.impl.SimpleEntryView<" + DATA_FULL_NAME
                    + "," + DATA_FULL_NAME + ">")) {
                setterString.append(PARAMETERS_PACKAGE + "EntryViewCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.client.impl.client.DistributedObjectInfo")) {
                setterString.append(PARAMETERS_PACKAGE + "DistributedObjectInfoCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.mapreduce.JobPartitionState")) {
                setterString.append(PARAMETERS_PACKAGE + "JobPartitionStateCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("javax.transaction.xa.Xid")) {
                setterString.append(PARAMETERS_PACKAGE + "XIDCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.cache.impl.CacheEventData")) {
                setterString.append(PARAMETERS_PACKAGE + "CacheEventDataCodec.encode(" + name + ", clientMessage);");
            } else if (type.equals("com.hazelcast.map.impl.querycache.event.SingleEventData")) {
                setterString.append(PARAMETERS_PACKAGE + "SingleEventDataCodec.encode(" + name + ", clientMessage);");
            } else if (type.startsWith("java.util.Map<")) {
                setterString.append(getMapSetterString(type, name));
            } else if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<")
                    || type.startsWith("java.util.Collection<")) {
                setterString.append(getCollectionSetterString(type, name));
            } else if (type.endsWith("[]")) {
                setterString.append(getArraySetterString(type, name));
            } else {
                setterString.append("clientMessage.set(" + name + ");");
            }
            return setterString.toString();
        }

        private String getArraySetterString(String type, String name) {
            String itemVariableName = name + "_item";

            String itemVariableType = CodeGenerationUtils.getTypeInsideData(type);

            StringBuilder builder = new StringBuilder();
            builder.append("clientMessage.set(" + name + ".length);\n        ");
            builder.append("for (" + itemVariableType + " " + itemVariableName + " : " + name + " ) {\n        ");
            builder.append("    " + resolveDataSetterStringJava(itemVariableType, itemVariableName) + "\n        ");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getCollectionSetterString(String type, String name) {
            String itemType = CodeGenerationUtils.getTypeInsideCollection(type);
            String itemVariableName = name + "_item";

            StringBuilder builder = new StringBuilder();
            builder.append("clientMessage.set(" + name + ".size());\n        ");
            builder.append("for (" + itemType + " " + itemVariableName + ": " + name + " ) {\n        ");
            builder.append("    " + resolveDataSetterStringJava(itemType, itemVariableName) + "\n        ");
            builder.append("}\n        ");
            return builder.toString();
        }

        private String getMapSetterString(String type, String name) {
            StringBuilder builder = new StringBuilder();

            String keyType = CodeGenerationUtils.getKeyTypeInsideMap(type);
            builder.append("java.util.Collection<" + keyType + "> " + name
                    + "_keySet = (java.util.Collection<" + keyType + ">) " + name + ".keySet();\n     ");
            builder.append(resolveDataSetterStringJava("java.util.Collection<" + keyType + "> ", name + "_keySet"));

            String valueType = CodeGenerationUtils.getValueTypeInsideMap(type);
            builder.append("java.util.Collection<" + valueType + "> " + name
                    + "_values = (java.util.Collection<" + valueType + "> )" + name + ".values();\n       ");
            builder.append(resolveDataSetterStringJava("java.util.Collection<" + valueType + ">", name + "_values"));

            return builder.toString();
        }
//
//        public String getDataSetterStringCSharp() {
//            String getterString;
//            if (type.equals(DATA_FULL_NAME  + " ")) {
//                getterString = "GetData";
//            } else if (type.equals("java.lang.String")) {
//                getterString = "GetStringUtf8";
//            } else if (type.equals("byte[]")) {
//                getterString = "GetByteArray";
//            } else if (type.equals("java.util.List<" + DATA_FULL_NAME  + " >")) {
//                getterString = "GetDataList";
//            } else if (type.equals("java.util.Set<" + DATA_FULL_NAME  + " >")) {
//                getterString = "GetDataSet";
//            } else if (type.equals("java.util.Collection<" + DATA_FULL_NAME  + " >")) {
//                getterString = "GetDataSet";
//            } else {
//                getterString = "Get" + CodeGenerationUtils.capitalizeFirstLetter(type);
//            }
//            return getterString;
//        }

        public String convertTypeToCSharp(String type) {
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

        @Override
        public String toString() {
            return "ParameterModel{"
                    + "name='" + name + '\''
                    + ", type='" + type + '\''
                    + ", lang=" + lang
                    + '}';
        }
    }
}
