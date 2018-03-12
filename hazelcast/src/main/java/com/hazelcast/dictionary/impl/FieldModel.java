package com.hazelcast.dictionary.impl;

import java.lang.reflect.Field;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

public class FieldModel {
    private final Field field;
    private final long offset;

    public FieldModel(Field field, long offset) {
        this.field = field;
        this.offset = offset;
    }

    public String name() {
        return field.getName();
    }

    public Class type() {
        return field.getType();
    }

    public Field field() {
        return field;
    }

    /**
     * Returns the offset of the field
     *
     * @return
     */
    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return name();
    }

    public   int size() {
        if (field.getType().equals(Byte.TYPE)) {
            return 1;
        } else if (field.getType().equals(Integer.TYPE)) {
            return INT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Long.TYPE)) {
            return LONG_SIZE_IN_BYTES;
        } else if (field.getType().equals(Short.TYPE)) {
            return SHORT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Float.TYPE)) {
            return FLOAT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Double.TYPE)) {
            return DOUBLE_SIZE_IN_BYTES;
        } else if (field.getType().equals(Boolean.TYPE)) {
            return BOOLEAN_SIZE_IN_BYTES;
        } else if (field.getType().equals(Character.TYPE)) {
            return CHAR_SIZE_IN_BYTES;
        } else {
            throw new RuntimeException(
                    "Unrecognized field type: field '" + field.getName() + "' ,type '" + field.getType().getName() + "' ");
        }
    }
}
