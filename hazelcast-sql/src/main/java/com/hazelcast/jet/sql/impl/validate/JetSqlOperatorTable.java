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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.sql.impl.aggregate.function.HazelcastAvgAggFunction;
import com.hazelcast.jet.sql.impl.aggregate.function.HazelcastCountAggFunction;
import com.hazelcast.jet.sql.impl.aggregate.function.HazelcastMinMaxAggFunction;
import com.hazelcast.jet.sql.impl.aggregate.function.HazelcastSumAggFunction;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.file.FileTableFunction;
import com.hazelcast.jet.sql.impl.connector.generator.SeriesGeneratorTableFunction;
import com.hazelcast.jet.sql.impl.connector.generator.StreamGeneratorTableFunction;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastCollectionTableOperator;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastMapValueConstructor;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastRowOperator;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastValuesOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public final class JetSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlSpecialOperator VALUES = new HazelcastValuesOperator();
    public static final SqlSpecialOperator ROW = new HazelcastRowOperator();
    public static final SqlSpecialOperator COLLECTION_TABLE = new HazelcastCollectionTableOperator("TABLE");
    public static final SqlSpecialOperator MAP_VALUE_CONSTRUCTOR = new HazelcastMapValueConstructor();

    // We use an operator that doesn't implement the HazelcastOperandTypeCheckerAware interface.
    // The reason is that HazelcastOperandTypeCheckerAware.prepareBinding() gets the operand type for
    // all operands, but in case of this operator we must not get it for the argument name operand, it's
    // an SQL identifier and Calcite tries to resolve it as a column name. The other parameter accepts
    // ANY type so there's no need for this
    public static final SqlSpecialOperator ARGUMENT_ASSIGNMENT = SqlStdOperatorTable.ARGUMENT_ASSIGNMENT;

    public static final SqlFunction SUM = new HazelcastSumAggFunction();
    public static final SqlFunction COUNT = new HazelcastCountAggFunction();
    public static final SqlFunction AVG = new HazelcastAvgAggFunction();
    public static final SqlFunction MIN = new HazelcastMinMaxAggFunction(SqlKind.MIN);
    public static final SqlFunction MAX = new HazelcastMinMaxAggFunction(SqlKind.MAX);

    public static final SqlFunction GENERATE_SERIES = new SeriesGeneratorTableFunction();
    public static final SqlFunction GENERATE_STREAM = new StreamGeneratorTableFunction();

    public static final SqlFunction CSV_FILE = new FileTableFunction("CSV_FILE", CsvFileFormat.FORMAT_CSV);
    public static final SqlFunction JSON_FLAT_FILE = new FileTableFunction("JSON_FLAT_FILE", SqlConnector.JSON_FLAT_FORMAT);
    public static final SqlFunction AVRO_FILE = new FileTableFunction("AVRO_FILE", AvroFileFormat.FORMAT_AVRO);
    public static final SqlFunction PARQUET_FILE = new FileTableFunction("PARQUET_FILE", ParquetFileFormat.FORMAT_PARQUET);

    private static final JetSqlOperatorTable INSTANCE = new JetSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private JetSqlOperatorTable() {
    }

    public static JetSqlOperatorTable instance() {
        return INSTANCE;
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
