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

package com.hazelcast.datastream.impl.query;

import com.hazelcast.datastream.impl.RecordModel;
import com.hazelcast.datastream.impl.RegionRunCodegen;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;

import java.lang.reflect.Field;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class QueryRegionRunCodegen extends RegionRunCodegen {

    public QueryRegionRunCodegen(String compilationId, Predicate query, RecordModel recordModel) {
        super(compilationId, query, recordModel);
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("import static com.hazelcast.datastream.impl.IndexOffsets.offsetInIndex;\n");
        add("\n");
        add("public class " + className() + " extends com.hazelcast.datastream.impl.query.QueryRegionRun {\n\n");
        add("    {indicesAvailable=%s;}\n\n", queryAnalyzer.getIndexedEqualPredicate() != null);
        addParamFields();
        addBindMethod();
        addRunFullScanMethod();
        addRunWithIndexMethod();
        add("}\n");
    }

    @Override
    public String className() {
        return "QueryRegionRun_" + compilationId;
    }

    private void addRunWithIndexMethod() {
        // we need a starting index..
        // so we need to known which index to use: this is available in the index offset
        // we need to have position in the index that determines the first record with that index

        add("    protected void runWithIndex(){\n");
        EqualPredicate equalPredicate = queryAnalyzer.getIndexedEqualPredicate();
        if (equalPredicate == null) {
            add("       throw new RuntimeException(\"No index\");\n");
            add("    }\n\n");
            return;
        }

        String attribute = equalPredicate.getAttributeName();
        Field attributeField = recordModel.getField(attribute);
        int indexStartOffset = recordModel.indexStartOffset(attribute);
        add("       //indexStartOffset %s\n", indexStartOffset);

        Comparable value = equalPredicate.getValue();
        add("       %s value=", attributeField.getType().getName());
        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.startsWith("$")) {
                String variableName = valueString.substring(1);
                add("param_" + variableName);
            } else {
                add(valueString);
            }
        } else {
            add(value.toString());
        }
        add(";\n");


        Class fieldType = attributeField.getType();
        int indexLength = recordModel.indexSize(attribute);
        int indexLengthDiv4 = indexLength / INT_SIZE_IN_BYTES;

        add("       long dataOffset=unsafe.getInt(indicesAddress+%s+", recordModel.indexStartOffset(attribute));
        add(INT_SIZE_IN_BYTES + "*");
        if (fieldType.equals(Boolean.TYPE)) {
            add("offsetInIndex(value)");
        } else if (fieldType.equals(Byte.TYPE)) {
            add("offsetInIndex(128+value)");
        } else if (fieldType.equals(Character.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else if (fieldType.equals(Short.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else if (fieldType.equals(Integer.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else if (fieldType.equals(Long.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else if (fieldType.equals(Float.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else if (fieldType.equals(Double.TYPE)) {
            add("offsetInIndex(value,%s)", indexLengthDiv4);
        } else {
            throw new RuntimeException("Unrecognized field type:" + attribute);
        }
        add(");\n");
        //add("       System.out.println(\"first\");\n");

        add("       List result=new LinkedList();\n");
        add("       this.result=result;\n");

        add("       while(dataOffset>=0){\n");
        add("           long recordAddress = dataAddress+dataOffset;\n");
        add("           if(");
        toCode(query, 0);
        add("){\n");
        addOnFound();
        //add("                 System.out.println(\"found\");\n");

        add("           }\n");
        //add("           System.out.println(\"before\");\n");
        add("           dataOffset = unsafe.getInt(recordAddress+%s);\n", recordModel.offsetNextForIndex(attribute));
        //add("           System.out.println(\"second\");\n");

        add("        }\n");
        add("    }\n\n");
    }

    private void addRunFullScanMethod() {
        add("    protected void runFullScan(){\n");
        add("       List result=new LinkedList();\n");
        add("       this.result=result;\n");
        add("       long recordAddress=dataAddress;\n");
        add("       for(int l=0;l<recordCount;l++){\n");
        add("           if(");
        toCode(query, 0);
        add("){\n");
        addOnFound();
        add("           }\n");
        add("           recordAddress+=%s;\n", recordModel.getSize());
        add("        }\n");
        add("    }\n\n");
    }

    private void addOnFound() {

        add("               " + recordModel.getRecordClassName() + " record=new " + recordModel.getRecordClassName() + "();\n");
        for (String fieldName : recordModel.getFields().keySet()) {
            add("               record." + fieldName + "=");
            addGetField(fieldName, 0);
            add(";\n");
        }
        //if(result.size()>0)
        add("               result.add(record);\n");
    }
}


