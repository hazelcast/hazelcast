/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hazelcast-specific SQL validator.
 */
public class HazelcastSqlValidator extends SqlValidatorImpl {
    public HazelcastSqlValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance
    ) {
        super(opTab, catalogReader, typeFactory, Config.DEFAULT.withSqlConformance(conformance));
    }

    @Override
    protected void addToSelectList(
        List<SqlNode> list,
        Set<String> aliases,
        List<Map.Entry<String, RelDataType>> fieldList,
        SqlNode exp,
        SelectScope scope,
        boolean includeSystemVars
    ) {
        if (isHiddenColumn(exp, scope)) {
            return;
        }

        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    private boolean isHiddenColumn(SqlNode node, SelectScope scope) {
        if (!(node instanceof SqlIdentifier)) {
            return false;
        }

        SqlIdentifier identifier = (SqlIdentifier) node;

        String fieldName = extractFieldName(identifier, scope);

        if (fieldName == null) {
            return false;
        }

        SqlValidatorTable table = scope.fullyQualify(identifier).namespace.getTable();

        if (table == null) {
            return false;
        }

        HazelcastTable unwrappedTable = table.unwrap(HazelcastTable.class);

        if (unwrappedTable == null) {
            return false;
        }

        return unwrappedTable.isHidden(fieldName);
    }

    private String extractFieldName(SqlIdentifier identifier, SelectScope scope) {
        SqlCall call = makeNullaryCall(identifier);

        if (call != null) {
            return null;
        }

        SqlQualified qualified = scope.fullyQualify(identifier);

        List<String> names = qualified.identifier.names;

        if (names.size() < 2) {
            return null;
        }

        return Util.last(names);
    }
}
