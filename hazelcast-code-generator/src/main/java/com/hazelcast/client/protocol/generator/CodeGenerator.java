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

    @Override
    public void init(ProcessingEnvironment env) {
        filer = env.getFiler();
        messager = env.getMessager();

        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_NONE);
        } catch (ClassNotFoundException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
        try {
            parameterTemplate = cfg.getTemplate("parameter-template-java.ftl");
            parameterTemplateCSharp = cfg.getTemplate("parameter-template-csharp.ftl");
            messageTypeTemplate = cfg.getTemplate("messagetype-template-java.ftl");
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> elements, RoundEnvironment env) {
        for (Element element : env.getElementsAnnotatedWith(GenerateParameters.class)) {
            generate((TypeElement) element);
        }
        return true;
    }

    public void generate(TypeElement classElement) {
        generateMessageTypeEnum(classElement);

        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (!enclosedElement.getKind().equals(ElementKind.METHOD)) {
                continue;
            }
            ExecutableElement methodElement = (ExecutableElement) enclosedElement;
            generateParameterClass(classElement, methodElement);
            //TODO: make this enabled by a parameter
            //generateParameterClassCSharp(classElement, methodElement);
        }
    }

    private void generateMessageTypeEnum(TypeElement classElement) {
        MessageTypeEnumModel clazz = new MessageTypeEnumModel(classElement);
        final String content = generateFromTemplate(messageTypeTemplate, clazz);
        saveClass(classElement, clazz.getPackageName(), clazz.getClassName(), content);
    }

    private void generateParameterClassCSharp(TypeElement classElement, ExecutableElement methodElement) {
        ParameterClassModel clazz = new ParameterClassModel(classElement, methodElement, Lang.CSHARP);
        final String content = generateFromTemplate(parameterTemplateCSharp, clazz);
        saveFile(classElement, clazz.getPackageName(), clazz.getClassName(), content);
    }

    private void generateParameterClass(TypeElement classElement, ExecutableElement methodElement) {
        ParameterClassModel clazz = new ParameterClassModel(classElement, methodElement, Lang.JAVA);
        final String content = generateFromTemplate(parameterTemplate, clazz);
        saveClass(classElement, clazz.getPackageName(), clazz.getClassName(), content);
    }

    private void saveClass(TypeElement classElement, String packageName, String className, String content) {
        JavaFileObject file;
        try {
            final String fullClassName = packageName + "." + className;
            file = filer.createSourceFile(fullClassName, classElement);
            file.openWriter().append(content).close();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage());
        }
    }

    private void saveFile(TypeElement classElement, String packageName, String className, String content) {
        FileObject file;
        try {
            final String fullName = className + ".cs";
            file = filer
                    .createResource(StandardLocation.locationFor(StandardLocation.SOURCE_OUTPUT.name()), packageName, fullName,
                            classElement);
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
