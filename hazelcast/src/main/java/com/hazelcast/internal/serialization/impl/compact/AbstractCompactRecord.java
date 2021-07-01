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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.TypeID;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.compact.schema.FieldOperations.fieldOperations;

public abstract class AbstractCompactRecord implements CompactRecord {

    abstract Schema getSchema();

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractCompactRecord)) {
            return false;
        }
        AbstractCompactRecord that = (AbstractCompactRecord) o;
        if (!that.getSchema().equals(getSchema())) {
            return false;
        }
        Set<String> thatFieldNames = that.getFieldNames();
        Set<String> thisFieldNames = getFieldNames();
        if (!Objects.equals(thatFieldNames, thisFieldNames)) {
            return false;
        }
        for (String fieldName : thatFieldNames) {
            TypeID thatFieldType = that.getFieldType(fieldName);
            TypeID thisFieldType = getFieldType(fieldName);
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
        int result = Objects.hash(getSchema());
        Set<String> thisFieldNames = getFieldNames();
        for (String fieldName : thisFieldNames) {
            TypeID fieldType = getFieldType(fieldName);
            result = 31 * result + fieldOperations(fieldType).hashCode(this, fieldName);
        }
        return result;
    }

    public final <T> T readAny(@Nonnull String fieldName) {
        TypeID type = getFieldType(fieldName);
        return (T) fieldOperations(type).readInSerializedForm(this, fieldName);
    }
}
