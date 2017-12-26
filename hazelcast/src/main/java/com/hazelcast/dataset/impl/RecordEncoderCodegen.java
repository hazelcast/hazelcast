package com.hazelcast.dataset.impl;

import java.lang.reflect.Field;
import java.util.List;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.lang.String.format;

public class RecordEncoderCodegen {
    private final StringBuffer codeBuffer = new StringBuffer();
    private final RecordModel recordModel;

    public RecordEncoderCodegen(RecordModel recordModel) {
        this.recordModel = recordModel;
    }

    public RecordEncoderCodegen append(String s, Object... args) {
        codeBuffer.append(format(s, (Object[]) args));
        return this;
    }

    public String className() {
        return recordModel.getRecordClass().getName().replace(".", "_").replace("$", "_") + "_Encoder";
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    public void generate() {
        append("import java.util.*;\n");
        append("import com.hazelcast.util.*;\n");
        append("import com.hazelcast.dataset.impl.*;\n");
        append("import static com.hazelcast.dataset.impl.IndexOffsets.offsetInIndex;\n");

        append("public class " + className() + " extends com.hazelcast.dataset.impl.RecordEncoder<%s> {\n\n",
                recordModel.getRecordClassName());
        generateConstructor();
        generateWriteRecordMethod();
        generateReadRecordMethod();
        append("}\n");
    }

    private void generateConstructor() {
        append("    public %s(RecordModel recordModel){\n", className());
        append("        super(recordModel);\n");
        append("    }\n\n");
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
        } else {
            throw new RuntimeException("Unrecognized field type:" + field);
        }
        sb.append(")");
        return sb.toString();
    }

    private void generateWriteRecordMethod() {
        append("    public void writeRecord(%s record, long segmentAddress, int recordOffset, long indicesAddress){\n",
                recordModel.getRecordClassName());

        // todo: with nullable fields, enums strings etc.. instead of doing a dumb copyMemory; we could generate per field
        append("        unsafe.copyMemory(record, recordDataOffset, null, segmentAddress+recordOffset, recordPayloadSize);\n");

        List<String> indexFields = recordModel.getIndexFields();
        if (!indexFields.isEmpty()) {
            append("\n");
            for (String fieldName : indexFields) {

                append("        //update the index %s\n", fieldName);

                // the address of the bucket in the index.
                append("        long bucketAddress_%s=indicesAddress+%s;\n", fieldName, generateIndexBucketOffset(fieldName));

                // first we read the existing value from the index
                append("        int next_%s=unsafe.getInt(bucketAddress_%s);\n", fieldName, fieldName);

                // then we update the next pointer in the record to point to the existing value
                append("        unsafe.putInt(segmentAddress+recordOffset+%s, next_%s);\n",
                        recordModel.offsetNextForIndex(fieldName), fieldName);

                // and then we write the newest value to the index.
                append("        unsafe.putInt(bucketAddress_%s,recordOffset);\n", fieldName);
                append("\n");
            }
        }
        append("    }\n\n");
    }

    private void generateReadRecordMethod() {
        append("    public void readRecord(%s record, long srcAddress){\n", recordModel.getRecordClassName());
        append("    }\n\n");
    }
}
