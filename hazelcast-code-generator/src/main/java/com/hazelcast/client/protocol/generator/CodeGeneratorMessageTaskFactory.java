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

import com.hazelcast.annotation.GenerateMessageTaskFactory;
import freemarker.cache.ClassTemplateLoader;
import freemarker.log.Logger;
import freemarker.template.Configuration;
import freemarker.template.Template;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportedAnnotationTypes("com.hazelcast.annotation.GenerateMessageTaskFactory")
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class CodeGeneratorMessageTaskFactory
        extends AbstractProcessor {

    private Filer filer;
    private Messager messager;
    private Template messageFactoryTemplate;

    private Types typeUtils;
    private Elements elementUtils;

    @Override
    public void init(ProcessingEnvironment env) {
        filer = env.getFiler();
        messager = env.getMessager();
        typeUtils = env.getTypeUtils();
        elementUtils = env.getElementUtils();

        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_LOG4J);
        } catch (ClassNotFoundException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
        try {
            messageFactoryTemplate = cfg.getTemplate("messagefactory-template-java.ftl");
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> elements, RoundEnvironment env) {
        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
        for (Element element : env.getElementsAnnotatedWith(GenerateMessageTaskFactory.class)) {
            map.put(element.toString(), addAllFromPackage((PackageElement) element) );
        }

        final String content = generateFromTemplate(messageFactoryTemplate, map);
        saveClass("com.hazelcast.client.impl.protocol", "MessageTaskFactoryImpl", content);
        return true;
    }

    private Map<String, String> addAllFromPackage(PackageElement packageElement) {
        Map<String, String> map = new HashMap<String, String>();
        for (Element enclosedElement : packageElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.CLASS)) {
                continue;
            }

            TypeElement typeElement = (TypeElement) enclosedElement;

            final Set<Modifier> modifiers = typeElement.getModifiers();
            if(modifiers.contains(Modifier.ABSTRACT) || modifiers.contains(Modifier.PROTECTED) || typeElement.getKind().isInterface()) {
                continue;
            }

            final DeclaredType superclass = (DeclaredType) typeElement.getSuperclass();

            final List<? extends TypeMirror> typeArguments = superclass.getTypeArguments();
            if (typeArguments.size() > 0) {
                final TypeMirror typeMirror = typeArguments.get(0);
                final String key = typeMirror.toString();
                if(key.endsWith("Parameters")) {
                    final String fullNameKey = key.startsWith("com.") ? key : "com.hazelcast.client.impl.protocol.parameters." + key;
                    map.put(fullNameKey, typeElement.toString());
                }
            }
        }
        return map;
    }

    private void saveClass(String packageName, String className, String content) {
        JavaFileObject file;
        try {
            final String fullClassName = packageName + "." + className;
            file = filer.createSourceFile(fullClassName);
            file.openWriter().append(content).close();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
    }

    private String generateFromTemplate(Template template, Object model) {
        String content = null;
        try {
            Map<String, Object> data = new HashMap();
            data.put("model", model);
            StringWriter writer = new StringWriter();
            template.process(data, writer);
            content = writer.toString();
        } catch (Exception e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        return content;
    }
}
