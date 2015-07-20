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

import com.hazelcast.annotation.Codec;
import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.annotation.Response;
import freemarker.cache.ClassTemplateLoader;
import freemarker.ext.beans.BeansWrapper;
import freemarker.log.Logger;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModelException;

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
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportedAnnotationTypes("com.hazelcast.annotation.GenerateCodec")
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class CodecCodeGenerator
        extends AbstractProcessor {

    private Filer filer;
    private Elements elementUtils;
    private Messager messager;
    private Template codecTemplate;
    //    private Template codecTemplateCSharp;
    private Template messageTypeTemplate;
    private Template messageTypeTemplateCSharp;
    private boolean csharpEnabled = Boolean.getBoolean("hazelcast.generator.csharp");
    private boolean cppEnabled = Boolean.getBoolean("hazelcast.generator.cpp");

    private Map<String, ExecutableElement> requestMap = new HashMap<String, ExecutableElement>();
    private Map<Integer, ExecutableElement> responseMap = new HashMap<Integer, ExecutableElement>();
    private Map<Integer, ExecutableElement> eventResponseMap = new HashMap<Integer, ExecutableElement>();

    @Override
    public void init(ProcessingEnvironment env) {

        filer = env.getFiler();
        messager = env.getMessager();
        elementUtils = env.getElementUtils();
        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_NONE);
        } catch (ClassNotFoundException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
        try {
            codecTemplate = cfg.getTemplate("codec-template-java.ftl");
            //codecTemplateCSharp = cfg.getTemplate("codec-template-csharp.ftl");
            messageTypeTemplate = cfg.getTemplate("messagetype-template-java.ftl");
            messageTypeTemplateCSharp = cfg.getTemplate("messagetype-template-csharp.ftl");
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> elements, RoundEnvironment env) {
        try {

            TypeElement te = elementUtils.getTypeElement("com.hazelcast.annotation.GenerateCodec");
            if(!elements.contains(te)) {
                return false;
            }
            for (Element element : env.getElementsAnnotatedWith(Codec.class)) {
                TypeElement classElement = (TypeElement) element;
                classElement.getAnnotationMirrors();
                Codec annotation = classElement.getAnnotation(Codec.class);
                if (annotation != null) {
                    try {
                        annotation.value();
                    } catch (MirroredTypeException mte) {
                        TypeMirror value = mte.getTypeMirror();
                        CodecModel.CUSTOM_CODEC_MAP.put(value.toString(), classElement);
                    }
                }
            }

            for (Element element : env.getElementsAnnotatedWith(GenerateCodec.class)) {
                register((TypeElement) element, Lang.JAVA);
            }

            for (Element element : env.getElementsAnnotatedWith(GenerateCodec.class)) {
                generateMessageTypeEnum((TypeElement) element, Lang.JAVA);
            }

            for (ExecutableElement element : requestMap.values()) {
                generateCodec(element, Lang.JAVA);
                if (csharpEnabled) {
                    //TODO :CSHARP
                }
                if (cppEnabled) {
                    //TODO: C++
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public void register(TypeElement classElement, Lang lang) {
        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.METHOD)) {
                continue;
            }
            ExecutableElement methodElement = (ExecutableElement) enclosedElement;

            short masterId = classElement.getAnnotation(GenerateCodec.class).id();

            final Request request = methodElement.getAnnotation(Request.class);
            if (request != null) {
                String id = CodeGenerationUtils.mergeIds(masterId, request.id());
                requestMap.put(id, methodElement);
                continue;
            }

            final Response response = methodElement.getAnnotation(Response.class);
            if (response != null) {
                responseMap.put(response.value(), methodElement);
                continue;
            }

            final EventResponse eventResponse = methodElement.getAnnotation(EventResponse.class);
            if (eventResponse != null) {
                eventResponseMap.put(eventResponse.value(), methodElement);
            }
        }
    }

    public void generateCodec(ExecutableElement methodElement, Lang lang) {
        CodecModel codecModel = createCodecModel(methodElement,lang);
        final String content;
        switch (lang) {
            case JAVA:
                content = generateFromTemplate(codecTemplate, codecModel);
                saveClass(codecModel.getPackageName(), codecModel.getClassName(), content);
                break;
            case CSHARP:
                //TODO
                //                content = generateFromTemplate(codecTemplateCSharp, codecModel);
                //                saveFile(codecModel.getClassName() + ".cs", codecModel.getPackageName(), content);
                //                break;
            case CPP:
                //TODO
                //content = generateFromTemplate(parameterTemplateCSharp, clazz);
                //saveFile(classElement, clazz.getPackageName(), clazz.getClassName(), content);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported language: " + lang);
        }
    }

    private CodecModel createCodecModel(ExecutableElement methodElement, Lang lang) {
        final TypeElement parent = (TypeElement) methodElement.getEnclosingElement();

        final Request methodElementAnnotation = methodElement.getAnnotation(Request.class);
        final int response = methodElementAnnotation.response();
        final int[] events = methodElementAnnotation.event();
        final boolean retryable = methodElementAnnotation.retryable();

        ExecutableElement responseElement = responseMap.get(response);

        List<ExecutableElement> eventElementList = new ArrayList<ExecutableElement>();
        if (events != null) {
            for (Integer eventType : events) {
                final ExecutableElement eventResponse = eventResponseMap.get(eventType);
                if (eventResponse != null) {
                    eventElementList.add(eventResponse);
                }
            }
        }

        return new CodecModel(parent, methodElement, responseElement, eventElementList, retryable, lang);
    }

    private void generateMessageTypeEnum(TypeElement classElement, Lang lang) {
        MessageTypeEnumModel clazz = new MessageTypeEnumModel(classElement, lang);
        if (clazz.isEmpty()) {
            return;
        }
        final String content;
        switch (lang) {
            case JAVA:
                content = generateFromTemplate(messageTypeTemplate, clazz);
                saveClass(clazz.getPackageName(), clazz.getClassName(), content);
                break;
            case CSHARP:
                content = generateFromTemplate(messageTypeTemplateCSharp, clazz);
                saveFile(clazz.getClassName() + ".cs", clazz.getPackageName(), content);
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

    private void generateProtocolDoc() {
//        StringBuilder sb = new StringBuilder();
//        for (ExecutableElement element : requestMap.values()) {
//            sb.append(generateFromTemplate(codecTemplate, codecModel));
//        }
//        saveClass(codecModel.getPackageName(), codecModel.getClassName(), content);

    }

    private void saveClass(String packageName, String className, String content) {
        JavaFileObject file;
        try {
            final String fullClassName = packageName + "." + className;
            file = filer.createSourceFile(fullClassName);
            file.openWriter().append(content).close();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.WARNING, e.getMessage());
        }
    }

    private void saveFile(String fileName, String packageName, String content) {
        FileObject file;
        try {
            final JavaFileManager.Location location = StandardLocation.locationFor(StandardLocation.SOURCE_OUTPUT.name());
            file = filer.createResource(location, packageName, fileName);
            file.openWriter().append(content).close();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.WARNING, e.getMessage());
        }
    }

    private String generateFromTemplate(Template template, Object model) {
        String content = null;
        try {
            Map<String, Object> data = new HashMap();
            setUtilModel(data);
            data.put("model", model);
            StringWriter writer = new StringWriter();
            template.process(data, writer);
            content = writer.toString();
        } catch (Exception e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        return content;
    }

    public static void setUtilModel(Map modelMap)
            throws TemplateModelException {
        BeansWrapper wrapper = BeansWrapper.getDefaultInstance();
        TemplateHashModel staticModels = wrapper.getStaticModels();
        TemplateHashModel statics = (TemplateHashModel) staticModels.get(CodeGenerationUtils.class.getName());
        modelMap.put("util", statics);
    }
}
