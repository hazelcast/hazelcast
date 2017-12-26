package com.hazelcast.dataseries.impl.query;

import com.hazelcast.dataseries.impl.RecordModel;
import com.hazelcast.dataseries.impl.SegmentRunCodegen;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;

import java.lang.reflect.Field;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class QuerySegmentRunCodegen extends SegmentRunCodegen {

    public QuerySegmentRunCodegen(String compilationId, Predicate query, RecordModel recordModel) {
        super(compilationId, query, recordModel);
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("import static com.hazelcast.dataseries.impl.IndexOffsets.offsetInIndex;\n");
        add("\n");
        add("public class " + className() + " extends com.hazelcast.dataseries.impl.query.QuerySegmentRun {\n\n");
        add("    {indicesAvailable=%s;}\n\n", queryAnalyzer.getIndexedEqualPredicate() != null);
        addParamFields();
        addBindMethod();
        addRunFullScanMethod();
        addRunWithIndexMethod();
        add("}\n");
    }

    @Override
    public String className() {
        return "QuerySegmentRun_" + compilationId;
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

        add("       long recordOffset=unsafe.getInt(indicesAddress+%s+", recordModel.indexStartOffset(attribute));
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

        add("       while(recordOffset>=0){\n");
        add("           long recordAddress = dataAddress+recordOffset;\n");
        add("           if(");
        toCode(query, 0);
        add("){\n");
        addOnFound();
        //add("                 System.out.println(\"found\");\n");

        add("           }\n");
        //add("           System.out.println(\"before\");\n");
        add("           recordOffset = unsafe.getInt(recordAddress+%s);\n", recordModel.offsetNextForIndex(attribute));
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


