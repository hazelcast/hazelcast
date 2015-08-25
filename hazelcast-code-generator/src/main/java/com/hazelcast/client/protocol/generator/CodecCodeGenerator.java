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

@SupportedAnnotationTypes({"com.hazelcast.annotation.GenerateCodec","com.hazelcast.annotation.Codec"})
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class CodecCodeGenerator
        extends AbstractProcessor {

    private Filer filer;
    private Elements elementUtils;
    private Messager messager;
    private Map<Lang, Template> codecTemplateMap = new HashMap<Lang, Template>();
    private Map<Lang, Template> messageTypeTemplateMap = new HashMap<Lang, Template>();
    private Map<TypeElement, Map<String, ExecutableElement>> requestMap = new HashMap<TypeElement, Map<String, ExecutableElement>>();
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
        for (Lang lang : Lang.values()) {
            boolean enabled = Boolean.getBoolean("hazelcast.generator." + lang.name().toLowerCase());
            if (enabled || lang == Lang.JAVA) {
                try {
                    Template codecTemplate = cfg.getTemplate("codec-template-" + lang.name().toLowerCase() + ".ftl");
                    codecTemplateMap.put(lang, codecTemplate);
                } catch (IOException e) {
                    messager.printMessage(Diagnostic.Kind.ERROR, "Cannot find template for lang:" + lang);
                }
                try {
                    Template messageTypeTemplate = cfg.getTemplate("messagetype-template-" + lang.name().toLowerCase() + ".ftl");
                    messageTypeTemplateMap.put(lang, messageTypeTemplate);
                } catch (IOException e) {
                    messager.printMessage(Diagnostic.Kind.WARNING, "Cannot find messagetype template for lang:" + lang);
                }
            }
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> elements, RoundEnvironment env) {
        try {
            TypeElement genCodecElement = elementUtils.getTypeElement("com.hazelcast.annotation.GenerateCodec");
            TypeElement codecElement = elementUtils.getTypeElement("com.hazelcast.annotation.Codec");
            if (!elements.contains(genCodecElement) || !elements.contains(codecElement)) {
                return false;
            }

            //PREPARE META DATA
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
                register((TypeElement) element);
            }
            //END
            for (Lang lang : codecTemplateMap.keySet()) {
                generateContent(lang);
            }
        } catch (Exception e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
            e.printStackTrace();
        }
        return true;
    }

    public void generateContent(Lang lang) {
        //GENERATE CONTENT
        Map<TypeElement, Map<String, CodecModel>> allCodecModel = createAllCodecModel(lang);

        Template messageTypeTemplate = messageTypeTemplateMap.get(lang);
        if (messageTypeTemplate != null) {
            for (Element element : allCodecModel.keySet()) {
                generateMessageTypeEnum((TypeElement) element, lang, messageTypeTemplate);
            }
        }

        Template codecTemplate = codecTemplateMap.get(lang);

        if(lang == Lang.MD) {
            generateDoc(allCodecModel, codecTemplate);
        } else {
            for (Map<String, CodecModel> map : allCodecModel.values()) {
                for (CodecModel model : map.values()) {
                    generateCodec(model, codecTemplate);
                }
            }
        }
    }

    public void register(TypeElement classElement) {
        HashMap<String, ExecutableElement> map = new HashMap<String, ExecutableElement>();
        requestMap.put(classElement, map);
        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.METHOD)) {
                continue;
            }
            ExecutableElement methodElement = (ExecutableElement) enclosedElement;

            short masterId = classElement.getAnnotation(GenerateCodec.class).id();

            final Request request = methodElement.getAnnotation(Request.class);
            if (request != null) {
                String id = CodeGenerationUtils.mergeIds(masterId, request.id());
                map.put(id, methodElement);
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

    private Map<TypeElement, Map<String, CodecModel>> createAllCodecModel(Lang lang) {
        Map<TypeElement, Map<String, CodecModel>> model = new HashMap<TypeElement, Map<String, CodecModel>>();

        for (Map.Entry<TypeElement, Map<String, ExecutableElement>> entry : requestMap.entrySet()) {
            HashMap<String, CodecModel> map = new HashMap<String, CodecModel>();
            TypeElement parent = entry.getKey();
            model.put(parent, map);


            for (Map.Entry<String, ExecutableElement> entrySub : entry.getValue().entrySet()) {
                ExecutableElement methodElement = entrySub.getValue();
                CodecModel codecModel = createCodecModel(methodElement, lang);
                String docComment = elementUtils.getDocComment(methodElement);
                if (null != docComment) {
                    codecModel.setComment(docComment);
                }
                map.put(entrySub.getKey(), codecModel);
            }
        }
        return model;
    }

    private CodecModel createCodecModel(ExecutableElement methodElement, Lang lang) {
        final TypeElement parent = (TypeElement) methodElement.getEnclosingElement();

        final Request methodElementAnnotation = methodElement.getAnnotation(Request.class);
        final int response = methodElementAnnotation.response();
        int[] events = null;
        try {
            events = methodElementAnnotation.event();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(parent.toString());
            System.err.println(methodElement.toString());
        }
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
        return new CodecModel(parent, methodElement, responseElement, eventElementList, retryable, lang, elementUtils);
    }

    public void generateCodec(CodecModel codecModel, Template codecTemplate) {
        final String content = generateFromTemplate(codecTemplate, codecModel);
        if (codecModel.getLang() == Lang.JAVA) {
            saveClass(codecModel.getPackageName(), codecModel.getClassName(), content);
        } else {
            //TODO
            saveFile(codecModel.getClassName() + "." + codecModel.getLang().name().toLowerCase(), codecModel.getPackageName(),
                    content);
        }
    }

    public void generateDoc(Object model, Template codecTemplate) {
        final String content = generateFromTemplate(codecTemplate, model);
        saveFile("protocol.md" , "document", content);
    }

    private void generateMessageTypeEnum(TypeElement classElement, Lang lang, Template messageTypeTemplate) {
        MessageTypeEnumModel model = new MessageTypeEnumModel(classElement, lang);
        if (model.isEmpty()) {
            return;
        }
        final String content = generateFromTemplate(messageTypeTemplate, model);
        saveContent(model,content);
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
            e.printStackTrace();
        }
        return content;
    }

    private void saveContent(Model codecModel, String content) {
        if (codecModel.getLang() == Lang.JAVA) {
            saveClass(codecModel.getPackageName(), codecModel.getClassName(), content);
        } else {
            //TODO
            saveFile(codecModel.getClassName() + "." + codecModel.getLang().name().toLowerCase(), codecModel.getPackageName(),
                    content);
        }
    }

    private void saveClass(String packageName, String className, String content) {
        JavaFileObject file;
        try {
            final String fullClassName = packageName + "." + className;
            file = filer.createSourceFile(fullClassName);
            file.openWriter().append(content).close();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.WARNING, e.getMessage());
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }

    public static void setUtilModel(Map modelMap)
            throws TemplateModelException {
        BeansWrapper wrapper = BeansWrapper.getDefaultInstance();
        TemplateHashModel staticModels = wrapper.getStaticModels();
        TemplateHashModel statics = (TemplateHashModel) staticModels.get(CodeGenerationUtils.class.getName());
        modelMap.put("util", statics);
    }
}
