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

    private final Lang lang;
    private String name;
    private String className;
    private String parentName;
    private String packageName;

    private int retryable;
    private short response;

    private final List<ParameterModel> requestParams = new LinkedList();
    private final List<ParameterModel> responseParams = new LinkedList();
    private final List<List> events = new LinkedList();

    public CodecModel(TypeElement parent, ExecutableElement methodElement, ExecutableElement responseElement,
                      List<ExecutableElement> eventElementList, boolean retryable, Lang lang) {
        this.retryable = retryable?1:0;
        this.lang = lang;

        name = methodElement.getSimpleName().toString();
        parentName = parent.getAnnotation(GenerateCodec.class).name();
        className =
                CodeGenerationUtils.capitalizeFirstLetter(parentName) + CodeGenerationUtils.capitalizeFirstLetter(name) + "Codec";
        packageName = "com.hazelcast.client.impl.protocol.codec";
        //CodeGenerationUtils.getPackageNameFromQualifiedName(parent.getQualifiedName().toString());

        //        if (lang != Lang.JAVA) {
        //            packageName = classElement.getAnnotation(GenerateParameters.class).ns();
        //        }

        response = methodElement.getAnnotation(Request.class).response();

        //request parameters
        for (VariableElement param : methodElement.getParameters()) {
            final Nullable nullable = param.getAnnotation(Nullable.class);
            if(nullable != null) {
                //TODO this parameter is nullable
            }
            param.asType().getKind().isPrimitive();
            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            pm.lang = lang;
            requestParams.add(pm);
        }

        //response parameters
        for (VariableElement param : responseElement.getParameters()) {
            final Nullable nullable = param.getAnnotation(Nullable.class);
            if(nullable != null) {
                //TODO this parameter is nullable
            }
            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            pm.lang = lang;
            responseParams.add(pm);
        }

        //event parameters
        for (ExecutableElement element : eventElementList) {
            List<ParameterModel> eventParam = new ArrayList<ParameterModel>();
            for (VariableElement param : element.getParameters()) {
                final Nullable nullable = param.getAnnotation(Nullable.class);
                if(nullable != null) {
                    //TODO this parameter is nullable
                }
                ParameterModel pm = new ParameterModel();
                pm.name = param.getSimpleName().toString();
                pm.type = param.asType().toString();
                pm.lang = lang;
                eventParam.add(pm);
            }
            events.add(eventParam);
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

    public short getResponse() {
        return response;
    }

    public List<ParameterModel> getRequestParams() {
        return requestParams;
    }

    public List<ParameterModel> getResponseParams() {
        return responseParams;
    }

    public List<List> getEvents() {
        return events;
    }

    public int getRetryable() {
        return retryable;
    }

    public static class ParameterModel {
        private String name;
        private String type;
        private Lang lang;

        public String getName() {
            return name;
        }

        public String getType() {
            if (lang == Lang.CSHARP) {
                return convertTypeToCSharp(type);
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
            if (type.equals("com.hazelcast.nio.serialization.Data")) {
                return "ParameterUtil.calculateDataSize(" + name + ")";
            } else if (type.equals("java.lang.String")) {
                return "ParameterUtil.calculateStringDataSize(" + name + ")";
            } else if (type.equals("byte[]")) {
                return "ParameterUtil.calculateByteArrayDataSize(" + name + ")";
            } else if (type.equals("int[]")) {
                return "ParameterUtil.calculateIntArrayDataSize(" + name + ")";
            } else if (type.equals("java.util.List<com.hazelcast.nio.serialization.Data>") || type
                    .equals("java.util.Set<com.hazelcast.nio.serialization.Data>") || type
                    .equals("java.util.Collection<com.hazelcast.nio.serialization.Data>")) {
                return "ParameterUtil.calculateCollectionDataSize(" + name + ")";
            } else if (type.equals("int") || type.equals("long") || type.equals("short") || type.equals("byte") || type
                    .equals("boolean")) {
                return "Bits." + type.toUpperCase() + "_SIZE_IN_BYTES";
            } else if(type.endsWith("[]")) {
                //TODO ARRAY
            }
            return CodeGenerationUtils.capitalizeFirstLetter(type) + "Codec.calculateDataSize(" + name + ")";

        }

        public String getSizeStringCSharp() {
            if (type.equals("com.hazelcast.nio.serialization.Data")) {
                return "ParameterUtil.CalculateDataSize(" + name + ")";
            } else if (type.equals("java.lang.String")) {
                return "ParameterUtil.CalculateStringDataSize(" + name + ")";
            } else if (type.equals("byte[]")) {
                return "ParameterUtil.CalculateByteArrayDataSize(" + name + ")";
            } else if (type.equals("java.util.List<com.hazelcast.nio.serialization.Data>") || type
                    .equals("java.util.Set<com.hazelcast.nio.serialization.Data>") || type
                    .equals("java.util.Collection<com.hazelcast.nio.serialization.Data>")) {
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

        public String getDataGetterStringJava() {
            String getterString;
            if (type.equals("com.hazelcast.nio.serialization.Data")) {
                getterString = "getData";
            } else if (type.equals("java.lang.String")) {
                getterString = "getStringUtf8";
            } else if (type.equals("byte[]")) {
                getterString = "getByteArray";
            } else if (type.equals("java.util.List<com.hazelcast.nio.serialization.Data>")) {
                getterString = "getDataList";
            } else if (type.equals("java.util.Set<com.hazelcast.nio.serialization.Data>")) {
                getterString = "getDataSet";
            } else if (type.equals("java.util.Collection<com.hazelcast.nio.serialization.Data>")) {
                getterString = "getDataSet";
            } else if (type.equals("com.hazelcast.map.impl.SimpleEntryView<com.hazelcast.nio.serialization.Data,com.hazelcast.nio.serialization.Data>")) {
                getterString = "SimpleEntryViewCodec.encode(client";
            } else {
                getterString = "get" + CodeGenerationUtils.capitalizeFirstLetter(type);
            }
            return getterString;
        }

        public String getDataGetterStringCSharp() {
            String getterString;
            if (type.equals("com.hazelcast.nio.serialization.Data")) {
                getterString = "GetData";
            } else if (type.equals("java.lang.String")) {
                getterString = "GetStringUtf8";
            } else if (type.equals("byte[]")) {
                getterString = "GetByteArray";
            } else if (type.equals("java.util.List<com.hazelcast.nio.serialization.Data>")) {
                getterString = "GetDataList";
            } else if (type.equals("java.util.Set<com.hazelcast.nio.serialization.Data>")) {
                getterString = "GetDataSet";
            } else if (type.equals("java.util.Collection<com.hazelcast.nio.serialization.Data>")) {
                getterString = "GetDataSet";
            } else {
                getterString = "Get" + CodeGenerationUtils.capitalizeFirstLetter(type);
            }
            return getterString;
        }

        public String convertTypeToCSharp(String type) {
            String getterString;
            if (type.equals("com.hazelcast.nio.serialization.Data")) {
                getterString = "IData";
            } else if (type.equals("java.lang.String")) {
                getterString = "string";
            } else if (type.equals("boolean")) {
                getterString = "bool";
            } else if (type.equals("java.util.List<com.hazelcast.nio.serialization.Data>")) {
                getterString = "IList<IData>";
            } else if (type.equals("java.util.Set<com.hazelcast.nio.serialization.Data>")) {
                getterString = "ISet<IData>";
            } else if (type.equals("java.util.Collection<com.hazelcast.nio.serialization.Data>")) {
                getterString = "ICollection<IData>";
            } else {
                getterString = type;
            }
            return getterString;
        }
    }
}
