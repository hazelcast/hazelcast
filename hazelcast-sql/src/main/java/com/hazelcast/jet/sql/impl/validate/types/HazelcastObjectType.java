/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableList;

public class HazelcastObjectType extends RelDataTypeImpl {
    private final String name;
    private final boolean nullable;
    /** Modifiable list of fields to support cyclic types. */
    private List<Field> fields = new ArrayList<>();
    /** Cached list of field names. */
    private List<String> fieldNames;

    /** Not usable until {@link #finalizeFields()} is called. */
    public HazelcastObjectType(String name) {
        this(name, true);
    }

    /** Not usable until {@link #finalizeFields()} is called. */
    public HazelcastObjectType(String name, boolean nullable) {
        super(null);
        this.name = name;
        this.nullable = nullable;
    }

    public String getTypeName() {
        return name;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.OTHER;
    }

    /**
     * @implNote This is called by {@link #computeDigest} and {@link #toString}, with
     * and without detail respectively. {@link #getFullTypeString}, {@link #equals} and
     * {@link #hashCode} uses the {@link #digest} generated via this method.
     * <p>
     * A previous implementation used to ignore {@code withDetail} and only incorporated
     * {@link #name} into the digest. This resulted in conflicts in {@link
     * RelDataTypeFactoryImpl#DATATYPE_CACHE} in the test environment. However, the
     * current state of user-defined types (UDTs) does not allow having different set of
     * fields for the same UDT in different mappings. If this is allowed in the future,
     * this implementation will ensure that there are no collisions in the cache.
     */
    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        if (withDetail) {
            generateFullTypeString(this, sb, new HashSet<>());
        } else {
            sb.append(name);
        }
    }

    private static void generateFullTypeString(HazelcastObjectType type, StringBuilder sb, Set<String> seen) {
        sb.append(type.name);
        if (seen.contains(type.name)) {
            return;
        }
        seen.add(type.name);
        sb.append('(');
        for (Iterator<Field> it = type.fields.iterator(); it.hasNext();) {
            RelDataTypeField field = it.next();
            sb.append(field.getName()).append(':');
            if (field.getType() instanceof HazelcastObjectType) {
                generateFullTypeString((HazelcastObjectType) field.getType(), sb, seen);
            } else {
                sb.append(field.getType().getFullTypeString());
            }
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }

    public void addField(Field field) {
        fields.add(field);
    }

    public void finalizeFields() {
        fields = List.of(fields.toArray(Field[]::new));
        super.computeDigest();
    }

    /**
     * @implNote {@code caseSensitive} is ignored since {@link #equals} compares the
     * {@link #digest} fields in a case-sensitive manner.
     */
    @Override
    @Nullable
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        assert !elideRecord;
        for (RelDataTypeField field : fields) {
            if (fieldName.equals(field.getName())) {
                return new RelDataTypeFieldImpl(fieldName, field.getIndex(), field.getType());
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RelDataTypeField> getFieldList() {
        return (List<RelDataTypeField>) (List<?>) fields;
    }

    @Override
    public List<String> getFieldNames() {
        if (fieldNames == null) {
            fieldNames = fields.stream().map(Field::getName).collect(toUnmodifiableList());
        }
        return fieldNames;
    }

    @Override
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public boolean isStruct() {
        return true;
    }

    public static class Field extends RelDataTypeFieldImpl {
        public Field(String name, int index, RelDataType type) {
            super(name, index, type);
        }
    }
}
