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
import com.hazelcast.jet.sql.impl.schema.HazelcastDynamicTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastSqlOperandMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;

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
        super(name, FileOperandMetadata.INSTANCE, arguments -> toTable(arguments, format), FileSqlConnector.INSTANCE);
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
