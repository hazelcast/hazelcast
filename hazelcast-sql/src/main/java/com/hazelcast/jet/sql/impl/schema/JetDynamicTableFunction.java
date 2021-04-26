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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;

/**
 * A table function return type of which is NOT known upfront and is determined during validation phase.
 */
public abstract class JetDynamicTableFunction implements JetTableFunction, TableFunction {

    private final SqlConnector connector;

    protected JetDynamicTableFunction(SqlConnector connector) {
        this.connector = connector;
    }

    @Override
    public boolean isStream() {
        return connector.isStream();
    }

    public final HazelcastTable toTable(RelDataType rowType) {
        return ((JetFunctionRelDataType) rowType).table();
    }

    @Override
    public final Type getElementType(List<Object> arguments) {
        return Object[].class;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
        HazelcastTable table = toTable(arguments);
        RelDataType rowType = table.getRowType(typeFactory);

        return new JetFunctionRelDataType(table, rowType);
    }

    protected abstract HazelcastTable toTable(List<Object> arguments);

    /**
     * The only purpose of this class is to be able to pass the {@code
     * HazelcastTable} object to place where the function is used.
     */
    private static final class JetFunctionRelDataType implements RelDataType {

        private final HazelcastTable table;
        private final RelDataType delegate;

        private JetFunctionRelDataType(HazelcastTable table, RelDataType delegate) {
            this.delegate = delegate;
            this.table = table;
        }

        private HazelcastTable table() {
            return table;
        }

        @Override
        public boolean isStruct() {
            return delegate.isStruct();
        }

        @Override
        public List<RelDataTypeField> getFieldList() {
            return delegate.getFieldList();
        }

        @Override
        public List<String> getFieldNames() {
            return delegate.getFieldNames();
        }

        @Override
        public int getFieldCount() {
            return delegate.getFieldCount();
        }

        @Override
        public StructKind getStructKind() {
            return delegate.getStructKind();
        }

        @Override
        public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
            return delegate.getField(fieldName, caseSensitive, elideRecord);
        }

        @Override
        public boolean isNullable() {
            return delegate.isNullable();
        }

        @Override
        public RelDataType getComponentType() {
            return delegate.getComponentType();
        }

        @Override
        public RelDataType getKeyType() {
            return delegate.getKeyType();
        }

        @Override
        public RelDataType getValueType() {
            return delegate.getValueType();
        }

        @Override
        public Charset getCharset() {
            return delegate.getCharset();
        }

        @Override
        public SqlCollation getCollation() {
            return delegate.getCollation();
        }

        @Override
        public SqlIntervalQualifier getIntervalQualifier() {
            return delegate.getIntervalQualifier();
        }

        @Override
        public int getPrecision() {
            return delegate.getPrecision();
        }

        @Override
        public int getScale() {
            return delegate.getScale();
        }

        @Override
        public SqlTypeName getSqlTypeName() {
            return delegate.getSqlTypeName();
        }

        @Override
        public SqlIdentifier getSqlIdentifier() {
            return delegate.getSqlIdentifier();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public String getFullTypeString() {
            return delegate.getFullTypeString();
        }

        @Override
        public RelDataTypeFamily getFamily() {
            return delegate.getFamily();
        }

        @Override
        public RelDataTypePrecedenceList getPrecedenceList() {
            return delegate.getPrecedenceList();
        }

        @Override
        public RelDataTypeComparability getComparability() {
            return delegate.getComparability();
        }

        @Override
        public boolean isDynamicStruct() {
            return delegate.isDynamicStruct();
        }
    }
}
