package com.hazelcast.dataseries.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.List;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.lang.String.format;

public class RecordEncoderCodegen {
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final StringBuffer codeBuffer = new StringBuffer();
    private final RecordModel recordModel;

    public RecordEncoderCodegen(RecordModel recordModel) {
        this.recordModel = recordModel;
    }

    public RecordEncoderCodegen add(String s, Object... args) {
        codeBuffer.append(format(s, (Object[]) args));
        return this;
    }

    public RecordEncoderCodegen add(Object arg) {
        codeBuffer.append(arg);
        return this;
    }

    public String className() {
        return recordModel.getRecordClass().getName().replace(".", "_").replace("$", "_") + "_Encoder";
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    public void generate() {
        add("import java.util.*;\n");
        add("import com.hazelcast.util.*;\n");
        add("import com.hazelcast.dataseries.impl.*;\n");
        add("import static com.hazelcast.dataseries.impl.IndexOffsets.offsetInIndex;\n");

        add("public class " + className() + " extends com.hazelcast.dataseries.impl.RecordEncoder<%s> {\n\n",
                recordModel.getRecordClassName());
        generateConstructor();
        generateNewInstanceMethod();
        generateWriteRecordMethod();
        generateReadRecordMethod();
        add("}\n");
    }

    private void generateConstructor() {
        add("    public %s(RecordModel recordModel){\n", className());
        add("        super(recordModel);\n");
        add("    }\n\n");
    }

    private void generateNewInstanceMethod() {
        add("    public %s newInstance(){\n", recordModel.getRecordClassName());
        add("        return new %s();\n", recordModel.getRecordClassName());
        add("    }\n\n");
    }

    public String generateIndexBucketOffset(String fieldName) {
        Field field = recordModel.getField(fieldName);
        Class fieldType = field.getType();
        int indexSize = recordModel.indexSize(fieldName);
        StringBuilder sb = new StringBuilder(recordModel.indexStartOffset(fieldName) + "+" + INT_SIZE_IN_BYTES + "*(");
        if (fieldType.equals(Boolean.TYPE)) {
            sb.append(format("offsetInIndex(record.%s)", fieldName));
        } else if (fieldType.equals(Byte.TYPE)) {
            sb.append(format("offsetInIndex(record.%s)", fieldName));
        } else if (fieldType.equals(Character.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Short.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Integer.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Long.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Float.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Double.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else if (fieldType.equals(Double.TYPE)) {
            sb.append(format("offsetInIndex(record.%s,%s)", fieldName, indexSize / INT_SIZE_IN_BYTES));
        } else {
            throw new RuntimeException("Unrecognized field type:" + field);
        }
        sb.append(")");
        return sb.toString();
    }

    private void generateWriteRecordMethod() {
        add("    public void writeRecord(%s record, long segmentAddress, int recordOffset, long indicesAddress){\n",
                recordModel.getRecordClassName());

        // todo: with nullable fields, enums strings etc.. instead of doing a dumb copyMemory; we could generate per field
        add("        unsafe.copyMemory(record, recordDataOffset, null, segmentAddress+recordOffset, recordPayloadSize);\n");

        List<String> indexFields = recordModel.getIndexFields();
        if (!indexFields.isEmpty()) {
            add("\n");
            for (String fieldName : indexFields) {

                add("        //update the index %s\n", fieldName);

                // the address of the bucket in the index.
                add("        long bucketAddress_%s=indicesAddress+%s;\n", fieldName, generateIndexBucketOffset(fieldName));

                // first we read the existing value from the index
                add("        int next_%s=unsafe.getInt(bucketAddress_%s);\n", fieldName, fieldName);

                // then we update the next pointer in the record to point to the existing value
                add("        unsafe.putInt(segmentAddress+recordOffset+%s, next_%s);\n",
                        recordModel.offsetNextForIndex(fieldName), fieldName);

                // and then we write the newest value to the index.
                add("        unsafe.putInt(bucketAddress_%s,recordOffset);\n", fieldName);
                add("\n");
            }
        }
        add("    }\n\n");
    }

    private void generateReadRecordMethod() {
        add("    public void readRecord(%s record, long segmentAddress, int recordOffset){\n", recordModel.getRecordClassName());
        add("        long recordAddress = segmentAddress + recordOffset;\n");

        for (String fieldName : recordModel.getFields().keySet()) {
            add("        record." + fieldName + "=");
            addGetField(fieldName);
            add(";\n");
        }

        add("    }\n\n");
    }


    protected void addGetField(String attributeName) {
        Field field = recordModel.getField(attributeName);
        long offset = unsafe.objectFieldOffset(field) - recordModel.getDataOffset();

        if (field.getType().equals(Byte.TYPE)) {
            add("unsafe.getByte(");
        } else if (field.getType().equals(Integer.TYPE)) {
            add("unsafe.getInt(");
        } else if (field.getType().equals(Long.TYPE)) {
            add("unsafe.getLong(");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.getShort(");
        } else if (field.getType().equals(Float.TYPE)) {
            add("unsafe.getFloat(");
        } else if (field.getType().equals(Double.TYPE)) {
            add("unsafe.getDouble(");
        } else if (field.getType().equals(Boolean.TYPE)) {
            add("unsafe.getBoolean(null,");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.getShort(");
        } else if (field.getType().equals(Character.TYPE)) {
            add("unsafe.getChar(");
        } else {
            throw new RuntimeException("Unhandled field comparison: '" + field.getType() + "' for attribute:" + attributeName);
        }

        add("recordAddress");
        add("+").add(offset);
        add(")");
    }
}
