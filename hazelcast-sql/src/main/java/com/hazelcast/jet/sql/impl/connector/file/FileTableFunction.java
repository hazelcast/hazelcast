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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.HazelcastDynamicTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastSqlOperandMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.security.Permission;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_OPTIONS;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.MAP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class FileTableFunction extends HazelcastDynamicTableFunction {

    private static final String SCHEMA_NAME_FILES = "files";
    private static final List<HazelcastTableFunctionParameter> PARAMETERS = asList(
            new HazelcastTableFunctionParameter(0, OPTION_PATH, VARCHAR, false, TypedOperandChecker.VARCHAR),
            new HazelcastTableFunctionParameter(1, OPTION_GLOB, VARCHAR, true, TypedOperandChecker.VARCHAR),
            new HazelcastTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, VARCHAR, true, TypedOperandChecker.VARCHAR),
            new HazelcastTableFunctionParameter(3, OPTION_OPTIONS, MAP, true, TypedOperandChecker.MAP)
    );

    public FileTableFunction(String name, String format) {
        super(name, FileOperandMetadata.INSTANCE, arguments -> toTable(arguments, format));
    }

    private static Table toTable(List<Object> arguments, String format) {
        Map<String, Object> options = toOptions(arguments, format);
        List<MappingField> fields = FileSqlConnector.resolveAndValidateFields(options, emptyList());
        return FileSqlConnector.createTable(SCHEMA_NAME_FILES, randomName(), options, fields);
    }

    /**
     * Takes a list of function arguments and converts it to equivalent options
     * that would be used if the file was declared using DDL.
     */
    private static Map<String, Object> toOptions(List<Object> arguments, String format) {
        assert arguments.size() == PARAMETERS.size();

        Map<String, Object> options = new HashMap<>();
        options.put(OPTION_FORMAT, format);
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments.get(i) != null) {
                options.put(PARAMETERS.get(i).name(), arguments.get(i));
            }
        }
        return options;
    }

    private static String randomName() {
        return SCHEMA_NAME_FILES + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    @Override
    public List<Permission> permissions(SqlCall call) {
        SqlNode astPath = findOperandByName(OPTION_PATH, call);
        if (astPath == null) {
            return Collections.emptyList();
        }
        if (astPath instanceof SqlLiteral) {
            String path = astPath.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
            return Collections.singletonList(ConnectorPermission.file(path, ActionConstants.ACTION_READ));
        } else if (astPath instanceof SqlDynamicParam) {
            // Note: it is pre-validation phase, we can't extract the path value from dynamic param yet.
            //  Better to have more strict permissions check here.
            return Collections.singletonList(ConnectorPermission.file("*", ActionConstants.ACTION_READ));
        }
        return Collections.emptyList();
    }

    private static SqlNode findOperandByName(String name, SqlCall call) {
        for (int i = 0; i < call.operandCount(); i++) {
            if (call.operand(i) instanceof SqlLiteral) {
                SqlLiteral literal = call.operand(i);
                if (name.equals(literal.toValue())) {
                    return literal;
                }
            }
        }
        return null;
    }

    private static final class FileOperandMetadata extends HazelcastSqlOperandMetadata {

        private static final FileOperandMetadata INSTANCE = new FileOperandMetadata();

        private FileOperandMetadata() {
            super(
                    PARAMETERS,
                    new HazelcastOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(ANY))
            );
        }

        @Override
        protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
            return true;
        }
    }
}
