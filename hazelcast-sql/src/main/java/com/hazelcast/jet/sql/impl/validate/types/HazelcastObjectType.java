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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class HazelcastObjectType extends RelDataTypeImpl {
    private final boolean nullable;
    private final String name;

    public HazelcastObjectType(final String name, final List<Field> fields) {
        super(fields);
        this.name = name;
        this.digest = "HZ_OBJECT(" + name + ")";
        this.nullable = true;
    }

    public HazelcastObjectType(final String name, final List<Field> fields, final boolean nullable) {
        super(fields);
        this.name = name;
        this.digest = "HZ_OBJECT(" + name + ")";
        this.nullable = nullable;
    }

    public String getTypeName() {
        return name;
    }

    @Override
    protected void generateTypeString(final StringBuilder sb, final boolean withDetail) {
        sb.append("HZ_OBJECT(")
                .append(name)
                .append(")");
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.OTHER;
    }

    @Override
    public @Nullable RelDataTypeField getField(final String fieldName, final boolean caseSensitive, final boolean elideRecord) {
        assert fieldList != null;
        RelDataTypeField fieldType = null;
        for (final RelDataTypeField field : fieldList) {
            if (fieldName.equals(field.getName())) {
                fieldType = new RelDataTypeFieldImpl(fieldName, field.getIndex(), field.getType());
                break;
            }
        }

        return fieldType;
    }

    @Override
    protected void computeDigest() {
        super.computeDigest();
    }

    public static class Field extends RelDataTypeFieldImpl {
        public Field(final String name, final int index, final RelDataType type) {
            super(name, index, type);
        }
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }
}
