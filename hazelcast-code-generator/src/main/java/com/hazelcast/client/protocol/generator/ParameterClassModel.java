package com.hazelcast.client.protocol.generator;

import com.hazelcast.annotation.GenerateParameters;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import java.util.LinkedList;
import java.util.List;

public class ParameterClassModel {

    private String name;
    private String className;
    private String parentName;
    private String packageName;
    private final List<ParameterModel> params = new LinkedList();

    public ParameterClassModel(TypeElement classElement, ExecutableElement methodElement, Lang lang) {
        name = methodElement.getSimpleName().toString();
        parentName = classElement.getAnnotation(GenerateParameters.class).name();
        className = CodeGenerationUtils.capitalizeFirstLetter(parentName) + CodeGenerationUtils.capitalizeFirstLetter(name)
                + "Parameters";
        packageName = CodeGenerationUtils.getPackageNameFromQualifiedName(classElement.getQualifiedName().toString());

        if (lang != Lang.JAVA) {
            packageName = classElement.getAnnotation(GenerateParameters.class).ns();
        }

        for (VariableElement param : methodElement.getParameters()) {
            ParameterModel pm = new ParameterModel();
            pm.name = param.getSimpleName().toString();
            pm.type = param.asType().toString();
            params.add(pm);
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

    public List<ParameterModel> getParams() {
        return params;
    }

    public static class ParameterModel {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getSizeString() {
            if (type.equals("java.lang.String")) {
                return "ParameterUtil.calculateStringDataSize(" + name + ")";
            } else if (type.equals("byte[]")) {
                return "ParameterUtil.calculateByteArrayDataSize(" + name + ")";
            }
            return "BitUtil.SIZE_OF_" + type.toUpperCase();
        }

        public String getDataGetterString() {
            if (type.equals("java.lang.String")) {
                return "getStringUtf8";
            } else if (type.equals("byte[]")) {
                return "getByteArray";
            }
            return "get" + CodeGenerationUtils.capitalizeFirstLetter(type);
        }
    }
}
