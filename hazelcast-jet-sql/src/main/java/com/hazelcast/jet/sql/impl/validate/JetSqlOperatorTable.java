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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.impl.connector.file.FileTableFunction;
import com.hazelcast.jet.sql.impl.connector.generator.SeriesGeneratorTableFunction;
import com.hazelcast.jet.sql.impl.connector.generator.StreamGeneratorTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetSqlUserDefinedTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

public final class JetSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlFunction CSV_FILE = from(FileTableFunction.CSV, "CSV_FILE");
    public static final SqlFunction JSON_FILE = from(FileTableFunction.JSON, "JSON_FILE");
    public static final SqlFunction AVRO_FILE = from(FileTableFunction.AVRO, "AVRO_FILE");
    public static final SqlFunction PARQUET_FILE = from(FileTableFunction.PARQUET, "PARQUET_FILE");
    public static final SqlFunction GENERATE_SERIES =
            from(SeriesGeneratorTableFunction.GENERATE_SERIES, "GENERATE_SERIES");
    public static final SqlFunction GENERATE_STREAM =
            from(StreamGeneratorTableFunction.GENERATE_STREAM, "GENERATE_STREAM");

    private static final JetSqlOperatorTable INSTANCE = new JetSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private JetSqlOperatorTable() {
    }

    static JetSqlOperatorTable instance() {
        return INSTANCE;
    }

    static SqlUserDefinedTableFunction from(JetTableFunction function, String name) {
        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;

        List<RelDataType> types = new ArrayList<>();
        List<SqlTypeFamily> families = new ArrayList<>();
        for (FunctionParameter parameter : function.getParameters()) {
            RelDataType type = parameter.getType(typeFactory);
            SqlTypeName typeName = type.getSqlTypeName();
            SqlTypeFamily typeFamily = typeName.getFamily();

            types.add(type);
            families.add(Util.first(typeFamily, SqlTypeFamily.ANY));
        }
        FamilyOperandTypeChecker typeChecker =
                OperandTypes.family(families, index -> function.getParameters().get(index).isOptional());

        return new JetSqlUserDefinedTableFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                ReturnTypes.CURSOR,
                InferTypes.explicit(types),
                typeChecker,
                types,
                function
        );
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier name,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operators,
            SqlNameMatcher nameMatcher
    ) {
        super.lookupOperatorOverloads(name, category, syntax, operators, SqlNameMatchers.withCaseSensitive(false));
    }
}
