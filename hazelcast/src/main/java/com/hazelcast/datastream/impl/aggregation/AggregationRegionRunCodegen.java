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

package com.hazelcast.datastream.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.DoubleSumAggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.datastream.AggregationRecipe;
import com.hazelcast.datastream.impl.RecordModel;
import com.hazelcast.datastream.impl.RegionRunCodegen;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class AggregationRegionRunCodegen extends RegionRunCodegen {

    private final Set<Field> extractedFields;
    private final Class<?> projectionClass;
    private final Aggregator aggregator;
    private final AggregationRecipe recipe;

    public AggregationRegionRunCodegen(String compilationId,
                                       AggregationRecipe recipe,
                                       RecordModel recordModel) {
        super(compilationId, recipe.getPredicate(), recordModel);
        this.recipe = recipe;
        try {
            this.projectionClass = getClass().getClassLoader().loadClass(recipe.getProjectionClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            Class<Aggregator> aggregatorClass = (Class<Aggregator>) getClass()
                    .getClassLoader().loadClass(recipe.getAggregatorClassName());
            this.aggregator = aggregatorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.extractedFields = extractedFields();
    }

    @Override
    public String className() {
        return "AggregationRegionRun_" + compilationId;
    }

    public Field field() {
        return extractedFields.iterator().next();
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("public class " + className() + " extends com.hazelcast.datastream.impl.aggregation.AggregationRegionRun {\n\n");
        addParamFields();
        addAggregatorField();
        addBindMethod();
        addResultMethod();
        addRunFullScanMethod();
        addRunWithIndexMethod();
        add("}\n");
    }

    private void addAggregatorField() {
        add("    private final " + aggregator.getClass().getName() + " aggregator = new " + aggregator.getClass().getName() + "();\n\n");
    }

    private void addResultMethod() {
        add("    public " + Aggregator.class.getName() + " result(){return aggregator;}\n\n");
    }

    private void addRunWithIndexMethod() {
        add("    protected void runWithIndex(){\n");
        add("    }\n\n");
    }

    private void addRunFullScanMethod() {
        add("    protected void runFullScan(){\n");

        if (aggregator instanceof CountAggregator) {
            add("       long result=0;\n");
        } else if (aggregator instanceof LongSumAggregator) {
            add("       long result=0;\n");
        } else if (aggregator instanceof MinAggregator) {
            add("       long result=Long.MAX_VALUE;\n");
        } else if (aggregator instanceof MaxAggregator) {
            add("       long result=Long.MIN_VALUE;\n");
        } else if (aggregator instanceof LongAverageAggregator) {
            add("       long sum=0;\n");
            add("       long count=0;\n");
        } else if (aggregator instanceof DoubleSumAggregator) {
            add("       double result=0;\n");
        } else {
            throw new RuntimeException("Unhandled type of aggregator:" + aggregator.getClass());
        }

        add("       long recordAddress=dataAddress;\n");
        add("       for(int l=0;l<recordCount;l++){\n");

        add("           if(");
        toCode(query, 0);
        add("){\n");

        if (aggregator instanceof CountAggregator) {
            add("               result+=1;\n");
        } else if (aggregator instanceof LongSumAggregator) {
            add("               result+=");
            addGetField(field().getName());
            add(";\n");
        } else if (aggregator instanceof LongAverageAggregator) {
            add("               count+=1;\n");
            add("               sum+=");
            addGetField(field().getName());
            add(";\n");
        } else if (aggregator instanceof MinAggregator) {
            add("               long value=");
            addGetField(field().getName());
            add(";\n");
            add("               if(value<result) result=value;\n");
        } else if (aggregator instanceof MaxAggregator) {
            add("               long value=");
            addGetField(field().getName());
            add(";\n");
            add("               if(value>result) result=value;\n");
        } else if (aggregator instanceof DoubleSumAggregator) {
            add("               result+=");
            addGetField(field().getName());
            add(";\n");
        }
        add("           }\n");
        add("           recordAddress+=%s;\n", recordModel.getSize());
        add("        }\n");

        if (aggregator instanceof LongAverageAggregator) {
            add("        aggregator.init(sum,count);\n");
        } else {
            add("        aggregator.accumulate(result);\n");
        }

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

            if (!f.getType().isAssignableFrom(recordField.getType())) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' has a different type compared to '" + recordField.getName() + "', "
                                + " expected:" + recordField.getType() + " found:" + f.getType());
            }

            fields.add(f);
        }

        return fields;
    }
}
