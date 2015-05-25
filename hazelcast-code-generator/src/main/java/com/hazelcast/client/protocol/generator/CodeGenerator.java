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

import com.hazelcast.annotation.GenerateParameters;
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
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SupportedAnnotationTypes("com.hazelcast.annotation.GenerateParameters")
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class CodeGenerator
        extends AbstractProcessor {

    private Filer filer;
    private Messager messager;
    private Template parameterTemplate;
    private Template parameterTemplateCSharp;
    private Template messageTypeTemplate;
    private Template messageTypeTemplateCSharp;

//    private boolean csharpEnabled = Boolean.getBoolean("hazelcast.generator.csharp");
//    private boolean cppEnabled = Boolean.getBoolean("hazelcast.generator.cpp");

    @Override
    public void init(ProcessingEnvironment env) {

        filer = env.getFiler();
        messager = env.getMessager();

        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_NONE);
        } catch (ClassNotFoundException e) {
            messager.printMessage(Diagnostic.Kind.WARNING, e.getMessage());
        }
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
        try {
            parameterTemplate = cfg.getTemplate("parameter-template-java.ftl");
            parameterTemplateCSharp = cfg.getTemplate("parameter-template-csharp.ftl");
            messageTypeTemplate = cfg.getTemplate("messagetype-template-java.ftl");
            messageTypeTemplateCSharp = cfg.getTemplate("messagetype-template-csharp.ftl");
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.WARNING, e.getMessage());
//            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> elements, RoundEnvironment env) {
        for (Element element : env.getElementsAnnotatedWith(GenerateParameters.class)) {
            generate((TypeElement) element, Lang.JAVA);
//            if (csharpEnabled) {
//                generate((TypeElement) element, Lang.CSHARP);
//            }
//            if (cppEnabled) {
//                generate((TypeElement) element, Lang.CPP);
//            }

        }
        return true;
    }

    public void generate(TypeElement classElement, Lang lang) {
        generateMessageTypeEnum(classElement, lang);
        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.METHOD)) {
                continue;
            }
            ExecutableElement methodElement = (ExecutableElement) enclosedElement;
            generateParameterClass(classElement, methodElement, lang);
        }
    }

    private void generateMessageTypeEnum(TypeElement classElement, Lang lang) {
        MessageTypeEnumModel clazz = new MessageTypeEnumModel(classElement, lang);
        final String content;
        switch (lang) {
            case JAVA:
                content = generateFromTemplate(messageTypeTemplate, clazz);
                saveClass(clazz.getPackageName(), clazz.getClassName(), content);
                break;
            case CSHARP:
                content = generateFromTemplate(messageTypeTemplateCSharp, clazz);
                saveFile(clazz.getClassName() + ".cs", clazz.getPackageName() , content);
                break;
            case CPP:
                //TODO
                //content = generateFromTemplate(messageTypeTemplateCpp, clazz);
                //saveFile(classElement, clazz.getPackageName(), clazz.getClassName(), content);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported language: " + lang);

        }
    }

    private void generateParameterClass(TypeElement classElement, ExecutableElement methodElement, Lang lang) {
        ParameterClassModel clazz = new ParameterClassModel(classElement, methodElement, lang);
        final String content;
        switch (lang) {
            case JAVA:
                content = generateFromTemplate(parameterTemplate, clazz);
                saveClass(clazz.getPackageName(), clazz.getClassName(), content);
                break;
            case CSHARP:
                content = generateFromTemplate(parameterTemplateCSharp, clazz);
                saveFile(clazz.getClassName() + ".cs", clazz.getPackageName() , content);
                break;
            case CPP:
                //TODO
                //content = generateFromTemplate(parameterTemplateCSharp, clazz);
                //saveFile(classElement, clazz.getPackageName(), clazz.getClassName(), content);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported language: " + lang);
        }
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

    private void saveFile(String fileName, String packageName, String content) {
        FileObject file;
        try {
            final JavaFileManager.Location location = StandardLocation.locationFor(StandardLocation.SOURCE_OUTPUT.name());
            file = filer.createResource(location, packageName, fileName);
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
