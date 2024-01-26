/*
 * Copyright 2024 Hazelcast Inc.
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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableList;

public class HazelcastObjectType extends RelDataTypeImpl {
    private final String name;
    private final boolean nullable;
    private ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
    /**
     * Makes it possible to finalize the fields after creating the type,
     * which is required to support cyclic types.
     */
    private List<Field> fields;
    /** Cached list of field names. */
    private List<String> fieldNames;

    /**
     * Creates a nullable type, which will not be usable until
     * {@linkplain #finalizeFields the fields are finalized}.
     */
    public HazelcastObjectType(String name) {
        this(name, true);
    }

    /** Not usable until {@linkplain #finalizeFields the fields are finalized}. */
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
            generateFullTypeString(sb, new HashSet<>());
        } else {
            sb.append(name);
        }
    }

    private void generateFullTypeString(StringBuilder sb, Set<String> seen) {
        escape(sb, name);
        if (seen.contains(name)) {
            return;
        }
        seen.add(name);
        sb.append('(');
        for (Iterator<RelDataTypeField> it = getFieldList().iterator(); it.hasNext();) {
            RelDataTypeField field = it.next();
            escape(sb, field.getName());
            sb.append(':');
            if (field.getType() instanceof HazelcastObjectType) {
                ((HazelcastObjectType) field.getType()).generateFullTypeString(sb, seen);
            } else {
                sb.append(field.getType().getFullTypeString());
            }
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }

    private static void escape(StringBuilder sb, String value) {
        for (char c : value.toCharArray()) {
            if (c == '(' || c == ':' || c == ',' || c == ')') {
                sb.append('\\');
            }
            sb.append(c);
        }
    }

    public void addField(Field field) {
        if (fieldsBuilder == null) {
            throw new IllegalStateException("Type fields are already finalized");
        }
        fieldsBuilder.add(field);
    }

    /**
     * @param types vertices of a type graph, which may be disconnected
     *
     * @implNote At finalization, the {@link #digest} is also computed, for which
     * the fields are traversed recursively. This requires all nested types to be
     * finalized beforehand. That's why the finalization is done in two passes.
     */
    public static void finalizeFields(Collection<HazelcastObjectType> types) {
        types.forEach(type -> {
            if (type.fieldsBuilder == null) {
                throw new IllegalStateException("Type fields are already finalized");
            }
            type.fields = type.fieldsBuilder.build();
            type.fieldsBuilder = null;
        });
        types.forEach(type -> type.computeDigest());
    }

    /**
     * @implNote {@code caseSensitive} is ignored since {@link #equals} compares the
     * {@link #digest} fields in a case-sensitive manner.
     */
    @Override
    @Nullable
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        assert !elideRecord;
        for (RelDataTypeField field : getFieldList()) {
            if (fieldName.equals(field.getName())) {
                return new RelDataTypeFieldImpl(fieldName, field.getIndex(), field.getType());
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RelDataTypeField> getFieldList() {
        if (fields == null) {
            throw new IllegalStateException("Type fields are not finalized");
        }
        return (List<RelDataTypeField>) (List<?>) fields;
    }

    @Override
    public List<String> getFieldNames() {
        if (fieldNames == null) {
            fieldNames = getFieldList().stream().map(RelDataTypeField::getName).collect(toUnmodifiableList());
        }
        return fieldNames;
    }

    @Override
    public int getFieldCount() {
        return getFieldList().size();
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
