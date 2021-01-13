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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.JetTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.jet.sql.impl.schema.UnknownStatistic;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PARQUET_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_OPTIONS;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public final class FileTableFunction extends JetTableFunction {

    public static final FileTableFunction CSV = new FileTableFunction(CSV_FORMAT, asList(
            new JetTableFunctionParameter(0, OPTION_PATH, SqlTypeName.VARCHAR, true),
            new JetTableFunctionParameter(1, OPTION_GLOB, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(3, OPTION_OPTIONS, SqlTypeName.MAP, false)
    ));

    public static final FileTableFunction JSON = new FileTableFunction(JSON_FORMAT, asList(
            new JetTableFunctionParameter(0, OPTION_PATH, SqlTypeName.VARCHAR, true),
            new JetTableFunctionParameter(1, OPTION_GLOB, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(3, OPTION_OPTIONS, SqlTypeName.MAP, false)
    ));

    public static final FileTableFunction AVRO = new FileTableFunction(AVRO_FORMAT, asList(
            new JetTableFunctionParameter(0, OPTION_PATH, SqlTypeName.VARCHAR, true),
            new JetTableFunctionParameter(1, OPTION_GLOB, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(3, OPTION_OPTIONS, SqlTypeName.MAP, false)
    ));

    public static final FileTableFunction PARQUET = new FileTableFunction(PARQUET_FORMAT, asList(
            new JetTableFunctionParameter(0, OPTION_PATH, SqlTypeName.VARCHAR, true),
            new JetTableFunctionParameter(1, OPTION_GLOB, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, SqlTypeName.VARCHAR, false),
            new JetTableFunctionParameter(3, OPTION_OPTIONS, SqlTypeName.MAP, false)
    ));

    private static final String SCHEMA_NAME_FILES = "files";

    private final String format;
    private final List<FunctionParameter> parameters;

    private FileTableFunction(String format, List<FunctionParameter> parameters) {
        super(FileSqlConnector.INSTANCE);

        this.format = format;
        this.parameters = parameters;
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return parameters;
    }

    @Override
    protected HazelcastTable toTable(List<Object> arguments) {
        Map<String, Object> options = toOptions(arguments);
        List<MappingField> fields = FileSqlConnector.resolveAndValidateFields(options, emptyList());
        Table table = FileSqlConnector.createTable(SCHEMA_NAME_FILES, randomName(), options, fields);

        return new HazelcastTable(table, UnknownStatistic.INSTANCE);
    }

    /**
     * Takes a list of function arguments and converts it to equivalent options
     * that would be used if the file was declared using DDL.
     */
    private Map<String, Object> toOptions(List<Object> arguments) {
        assert arguments.size() == parameters.size();

        Map<String, Object> options = new HashMap<>();
        options.put(OPTION_FORMAT, format);
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments.get(i) != null) {
                options.put(parameters.get(i).getName(), arguments.get(i));
            }
        }
        return options;
    }

    private static String randomName() {
        return "file_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
