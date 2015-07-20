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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.client.protocol.generator.CodeGenerationUtils.convertTypeToCSharp;

public class CodecModel {

    static final Map<String, TypeElement> CUSTOM_CODEC_MAP = new HashMap<String, TypeElement>();

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

    //TEST ONLY MOCKUP CONSTRUCTOR
    public CodecModel(boolean mockup) {
        this.retryable = 1;
        this.lang = Lang.JAVA;

        name = "put";
        parentName = "Map";
        className =
                CodeGenerationUtils.capitalizeFirstLetter(parentName) + CodeGenerationUtils.capitalizeFirstLetter(name) + "Codec";
        packageName = "com.hazelcast.client.impl.protocol.codec";

        response = 104;

        //request parameters

        ParameterModel pm = new ParameterModel();
        pm.name = "name";
        pm.type = "java.lang.String";
        pm.lang = Lang.JAVA;
        pm.nullable = true;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "val";
        pm.type = "int";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "address";
        pm.type = "com.hazelcast.nio.Address";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "arr";
        pm.type = "int[]";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "setD";
        pm.type = "java.util.Set<" + CodeGenerationUtils.DATA_FULL_NAME + ">";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "mapIS";
        pm.type = "java.util.Map<java.lang.Integer, java.lang.String>";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "mapDD";
        pm.type = "java.util.Map<" + CodeGenerationUtils.DATA_FULL_NAME + ", " + CodeGenerationUtils.DATA_FULL_NAME + ">";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        requestParams.add(pm);

        pm = new ParameterModel();
        pm.name = "entryView";
        pm.type = "com.hazelcast.map.impl.SimpleEntryView<" + CodeGenerationUtils.DATA_FULL_NAME + ", "
                + CodeGenerationUtils.DATA_FULL_NAME + ">";
        pm.lang = Lang.JAVA;
        pm.nullable = true;
        requestParams.add(pm);

        //response parameters
        pm = new ParameterModel();
        pm.name = "name";
        pm.type = "long";
        pm.lang = Lang.JAVA;
        pm.nullable = false;
        responseParams.add(pm);

        EventModel eventModel = new EventModel();
        eventModel.type = 104;
        eventModel.name = "";

        List<ParameterModel> eventParam = new ArrayList<ParameterModel>();
        pm = new ParameterModel();
        pm.name = "name";
        pm.type = "java.lang.String";
        pm.lang = Lang.JAVA;
        pm.nullable = true;
        eventParam.add(pm);
        eventModel.eventParams = eventParam;
        events.add(eventModel);
    }

    public CodecModel(TypeElement parent, ExecutableElement methodElement, ExecutableElement responseElement,
                      List<ExecutableElement> eventElementList, boolean retryable, Lang lang) {
        this.retryable = retryable ? 1 : 0;
        this.lang = lang;

        name = methodElement.getSimpleName().toString();
        parentName = parent.getAnnotation(GenerateCodec.class).name();
        className =
                CodeGenerationUtils.capitalizeFirstLetter(parentName) + CodeGenerationUtils.capitalizeFirstLetter(name) + "Codec";
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
            pm.nullable = nullable != null;
            requestParams.add(pm);
        }

        //response parameters
        for (VariableElement param : responseElement.getParameters()) {
            final Nullable nullable = param.getAnnotation(Nullable.class);
            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            pm.lang = lang;
            pm.nullable = nullable != null;
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
                pm.nullable = nullable != null;
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
        private boolean nullable;

        public String getName() {
            return name;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getType() {
            if (lang == Lang.CSHARP) {
                return convertTypeToCSharp(type);
            }
            if (type.startsWith("java.util.List<") || type.startsWith("java.util.Set<") || type
                    .startsWith("java.util.Collection<")) {
                return type.replaceAll("java.util.*<(.*)>", "java.util.Collection<$1>");
            }
            return type;
        }

    }
}
