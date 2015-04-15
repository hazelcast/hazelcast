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

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import java.util.LinkedList;
import java.util.List;

public class MessageTypeEnumModel {

    private static final int BYTE_BIT_COUNT = 8;
    private String name;
    private String className;
    private String packageName;

    private final List<ParameterModel> params = new LinkedList();

    public MessageTypeEnumModel(TypeElement classElement) {
        name = classElement.getAnnotation(GenerateParameters.class).name();
        className = CodeGenerationUtils.capitalizeFirstLetter(name) + "MessageType";
        packageName = CodeGenerationUtils.getPackageNameFromQualifiedName(classElement.getQualifiedName().toString());

        short masterId = classElement.getAnnotation(GenerateParameters.class).id();

        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.METHOD)) {
                continue;
            }
            ExecutableElement methodElement = (ExecutableElement) enclosedElement;

            ParameterModel pm = new ParameterModel();
            pm.name = methodElement.getSimpleName().toString();
            final String s = Integer
                    .toHexString((masterId << BYTE_BIT_COUNT) + methodElement.getAnnotation(EncodeMethod.class).id());
            pm.id = s.length() == 3 ? "0x0" + s : "0x" + s;
            params.add(pm);
        }
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getPackageName() {
        return packageName;
    }

    public List<ParameterModel> getParams() {
        return params;
    }

    public static class ParameterModel {
        private String name;
        private String id;

        public String getName() {
            return name;
        }

        public String getId() {
            return id;
        }

    }
}
