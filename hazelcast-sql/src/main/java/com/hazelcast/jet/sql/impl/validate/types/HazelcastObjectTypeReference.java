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
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Temporary reference type used to support CRec (Circularly-recurrent) types e.g.
 * Class A { B b; }
 * Class B { C c; }
 * Class C { A a; }
 * In this hierarchy instead of supplying actual type for columns b, c, a, this type will be inserted and later on
 * the original's will be set to respective HazelcastObjectTypes to fix the references.
 */
public class HazelcastObjectTypeReference implements RelDataType {
    private HazelcastObjectType original;

    public RelDataType getOriginal() {
        return original;
    }

    public void setOriginal(final HazelcastObjectType original) {
        this.original = original;
    }

    @Override
    public boolean isStruct() {
        assert original != null;
        return original.isStruct();
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
        assert original != null;
        return original.getFieldList();
    }

    @Override
    public List<String> getFieldNames() {
        assert original != null;
        return original.getFieldNames();
    }

    @Override
    public int getFieldCount() {
        assert original != null;
        return original.getFieldCount();
    }

    @Override
    public StructKind getStructKind() {
        assert original != null;
        return original.getStructKind();
    }

    @Override
    public @Nullable RelDataTypeField getField(final String fieldName, final boolean caseSensitive, final boolean elideRecord) {
        assert original != null;
        return original.getField(fieldName, caseSensitive, elideRecord);
    }

    @Override
    public boolean isNullable() {
        assert original != null;
        return original.isNullable();
    }

    @Override
    public @Nullable RelDataType getComponentType() {
        assert original != null;
        return original.getComponentType();
    }

    @Override
    public @Nullable RelDataType getKeyType() {
        assert original != null;
        return original.getKeyType();
    }

    @Override
    public @Nullable RelDataType getValueType() {
        assert original != null;
        return original.getValueType();
    }

    @Override
    public @Nullable Charset getCharset() {
        assert original != null;
        return original.getCharset();
    }

    @Override
    public @Nullable SqlCollation getCollation() {
        assert original != null;
        return original.getCollation();
    }

    @Override
    public @Nullable SqlIntervalQualifier getIntervalQualifier() {
        assert original != null;
        return original.getIntervalQualifier();
    }

    @Override
    public int getPrecision() {
        assert original != null;
        return original.getPrecision();
    }

    @Override
    public int getScale() {
        assert original != null;
        return original.getScale();
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        assert original != null;
        return original.getSqlTypeName();
    }

    @Override
    public @Nullable SqlIdentifier getSqlIdentifier() {
        assert original != null;
        return original.getSqlIdentifier();
    }

    @Override
    public String getFullTypeString() {
        assert original != null;
        return original.getFullTypeString();
    }

    @Override
    public RelDataTypeFamily getFamily() {
        assert original != null;
        return original.getFamily();
    }

    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        assert original != null;
        return original.getPrecedenceList();
    }

    @Override
    public RelDataTypeComparability getComparability() {
        assert original != null;
        return original.getComparability();
    }

    @Override
    public boolean isDynamicStruct() {
        assert original != null;
        return original.isDynamicStruct();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        assert original != null;
        return obj != null && obj.equals(original);
    }

    @Override
    public int hashCode() {
        assert original != null;
        return original.hashCode();
    }

    @Override
    public String toString() {
        assert original != null;
        return original.toString();
    }

    @Override
    public boolean equalsSansFieldNames(@Nullable final RelDataType that) {
        assert original != null;
        return original.equalsSansFieldNames(that);
    }
}
