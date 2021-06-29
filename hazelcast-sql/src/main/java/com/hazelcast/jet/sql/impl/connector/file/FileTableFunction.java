/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.schema.JetDynamicTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

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

public final class FileTableFunction extends JetDynamicTableFunction {

    private static final String SCHEMA_NAME_FILES = "files";
    private static final List<JetTableFunctionParameter> PARAMETERS = asList(
            new JetTableFunctionParameter(0, OPTION_PATH, SqlTypeName.VARCHAR, TypedOperandChecker.VARCHAR),
            new JetTableFunctionParameter(1, OPTION_GLOB, SqlTypeName.VARCHAR, TypedOperandChecker.VARCHAR),
            new JetTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, SqlTypeName.VARCHAR, TypedOperandChecker.VARCHAR),
            new JetTableFunctionParameter(3, OPTION_OPTIONS, SqlTypeName.MAP, TypedOperandChecker.MAP)
    );

    public FileTableFunction(String name, String format) {
        super(
                name,
                PARAMETERS,
                arguments -> toTable(arguments, format),
                new HazelcastOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(ANY)),
                FileSqlConnector.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(1, PARAMETERS.size());
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
}
