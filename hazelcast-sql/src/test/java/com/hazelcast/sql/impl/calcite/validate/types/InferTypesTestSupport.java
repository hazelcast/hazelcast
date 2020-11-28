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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlTrimFunction;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import java.util.List;

public class InferTypesTestSupport {
    protected static RelDataType type(SqlTypeName typeName) {
        return HazelcastTypeFactory.INSTANCE.createSqlType(typeName);
    }

    protected static RelDataType typeUnknown() {
        return HazelcastTypeFactory.INSTANCE.createUnknownType();
    }

    protected static SqlCallBinding createBinding() {
        SqlParserPos parserPos = new SqlParserPos(0, 0);
        SqlCall call = new SqlBasicCall(new HazelcastSqlTrimFunction(), new SqlNode[0], parserPos);

        HazelcastSqlValidator validator = new HazelcastSqlValidator(
            new MockCatalogReader(),
            HazelcastTypeFactory.INSTANCE,
            HazelcastSqlConformance.INSTANCE
        );

        return new SqlCallBinding(validator, null, call);
    }

    private static class MockCatalogReader implements SqlValidatorCatalogReader {
        @Override
        public SqlValidatorTable getTable(List<String> names) {
            return null;
        }

        @Override
        public RelDataType getNamedType(SqlIdentifier typeName) {
            return null;
        }

        @Override
        public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
            return null;
        }

        @Override
        public List<List<String>> getSchemaPaths() {
            return null;
        }

        @Override
        public RelDataTypeField field(RelDataType rowType, String alias) {
            return null;
        }

        @Override
        public SqlNameMatcher nameMatcher() {
            return SqlNameMatchers.liberal();
        }

        @Override
        public boolean matches(String string, String name) {
            return false;
        }

        @Override
        public RelDataType createTypeFromProjection(RelDataType type, List<String> columnNameList) {
            return null;
        }

        @Override
        public boolean isCaseSensitive() {
            return false;
        }

        @Override
        public CalciteSchema getRootSchema() {
            return null;
        }

        @Override
        public CalciteConnectionConfig getConfig() {
            return null;
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }
    }
}
