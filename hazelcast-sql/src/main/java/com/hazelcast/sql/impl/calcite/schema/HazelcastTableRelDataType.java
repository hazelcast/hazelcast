/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Data type of a specific table.
 */
public class HazelcastTableRelDataType extends RelDataTypeImpl {
    /** Type name. */
    private static final String TYPE_NAME = "HazelcastRow";

    /** Type factory. */
    private final RelDataTypeFactory typeFactory;

    /** Dynamic fields. */
    private final HazelcastTableFields fields;

    public HazelcastTableRelDataType(RelDataTypeFactory typeFactory, HazelcastTableFields fields) {
        this.typeFactory = typeFactory;
        this.fields = fields;

        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("(").append(TYPE_NAME).append(getFieldNames()).append(")");
    }

    @Override
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        RelDataTypeField res = fields.getField(typeFactory, fieldName, caseSensitive);

        if (fields.isFieldCreated()) {
            computeDigest();
        }

        return res;
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
        return fields.getFieldList();
    }

    @Override
    public List<String> getFieldNames() {
        return fields.getFieldNames();
    }

    @Override
    public int getFieldCount() {
        return fields.getFieldCount();
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ROW;
    }

    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new SqlTypeExplicitPrecedenceList(ImmutableList.of());
    }

    @Override
    public boolean isStruct() {
        return true;
    }

    @Override
    public boolean isDynamicStruct() {
        return true;
    }
}
