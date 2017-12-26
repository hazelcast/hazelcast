/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.projection;

import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.RecordModel;
import com.hazelcast.datastream.impl.RegionRunCodegen;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class ProjectionRegionRunCodegen extends RegionRunCodegen {

    private final ProjectionRecipe projectionRecipe;
    private final Class<?> projectionClass;

    public ProjectionRegionRunCodegen(String compilationId,
                                      ProjectionRecipe projectionRecipe,
                                      RecordModel recordModel) {
        super(compilationId, projectionRecipe.getPredicate(), recordModel);

        this.projectionRecipe = projectionRecipe;
        try {
            this.projectionClass = getClass().getClassLoader().loadClass(projectionRecipe.getClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String className() {
        return "ProjectionRegionRun_" + compilationId;
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("public class " + className() + " extends com.hazelcast.datastream.impl.projection.ProjectionRegionRun {\n\n");
        addParamFields();
        addBindMethod();
        addRunFullScanMethod();
        addRunWithIndexMethod();
        add("}\n");
    }

    private void addRunWithIndexMethod() {
        add("    protected void runWithIndex(){\n");
        add("    }\n\n");
    }

    private void addRunFullScanMethod() {
        add("    protected void runFullScan(){\n");
        add("       long recordAddress=dataAddress;\n");
        if (projectionRecipe.isReusePojo()) {
            add("       " + projectionClass.getName() + " object=new " + projectionClass.getName() + "();\n");
        }
        add("       for(int l=0;l<recordCount;l++){\n");
        add("           if(");
        toCode(query, 0);
        add("){\n");
        if (!projectionRecipe.isReusePojo()) {
            add("               " + projectionClass.getName() + " object=new " + projectionClass.getName() + "();\n");
        }

        for (Field field : extractedFields()) {
            add("               object.").add(field.getName()).add("=");
            addGetField(field.getName(), 0);
            add(";\n");
        }
        add("               consumer.accept(object);\n");
        add("           }\n");
        add("           recordAddress+=%s;\n", recordModel.getSize());
        add("        }\n");
        add("    }\n\n");
    }

    private Set<Field> extractedFields() {
        Set<Field> fields = new HashSet<Field>();

        for (Field f : projectionClass.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            Field recordField = recordModel.getField(f.getName());
            if (recordField == null) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' is not found on value-class '" + recordModel.getRecordClass() + "'");
            }

            if (!recordField.getType().equals(f.getType())) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' has a different type compared to '" + recordModel.getRecordClass() + "'");
            }

            fields.add(f);
        }

        return fields;
    }
}
