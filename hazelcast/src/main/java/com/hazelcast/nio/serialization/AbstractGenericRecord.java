/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.json.JsonEscape;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;

/**
 * Implementation of GenericRecord interface to give common equals and hashcode implementation
 */
public abstract class AbstractGenericRecord implements GenericRecord {

    protected abstract Object getClassIdentifier();

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractGenericRecord)) {
            return false;
        }
        AbstractGenericRecord that = (AbstractGenericRecord) o;
        if (!that.getClassIdentifier().equals(getClassIdentifier())) {
            return false;
        }
        Set<String> thatFieldNames = that.getFieldNames();
        Set<String> thisFieldNames = getFieldNames();
        if (!Objects.equals(thatFieldNames, thisFieldNames)) {
            return false;
        }
        for (String fieldName : thatFieldNames) {
            FieldType thatFieldType = that.getFieldType(fieldName);
            FieldType thisFieldType = getFieldType(fieldName);
            if (!thatFieldType.equals(thisFieldType)) {
                return false;
            }
            if (thatFieldType.isArrayType()) {
                if (!Objects.deepEquals(readAny(fieldName), that.readAny(fieldName))) {
                    return false;
                }
            } else {
                if (!Objects.equals(readAny(fieldName), that.readAny(fieldName))) {
                    return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        int result = Objects.hash(getClassIdentifier());
        Set<String> thisFieldNames = getFieldNames();
        for (String fieldName : thisFieldNames) {
            FieldType fieldType = getFieldType(fieldName);
            result = 31 * result + fieldOperations(fieldType).hashCode(this, fieldName);
        }
        return result;
    }

    public final <T> T readAny(@Nonnull String fieldName) {
        FieldType type = getFieldType(fieldName);
        return (T) fieldOperations(type).readGenericRecordOrPrimitive(this, fieldName);
    }

    /**
     * Utility method to build a json String representation of the fields together with field names
     *
     * @param stringBuilder to be populated via json representation
     */
    protected void writeFieldsToStringBuilder(StringBuilder stringBuilder) {
        Set<String> fieldNames = getFieldNames();
        stringBuilder.append("{");
        int size = fieldNames.size();
        int i = 0;
        for (String fieldName : fieldNames) {
            i++;
            JsonEscape.writeEscaped(stringBuilder, fieldName);
            stringBuilder.append(": ");
            FieldType fieldType = getFieldType(fieldName);
            fieldOperations(fieldType).writeJsonFormattedField(stringBuilder, this, fieldName);
            if (size != i) {
                stringBuilder.append(",\n");
            }
        }
        stringBuilder.append("}");
    }
}
