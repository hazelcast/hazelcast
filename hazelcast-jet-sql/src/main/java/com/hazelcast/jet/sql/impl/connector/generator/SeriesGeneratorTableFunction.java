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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.JetTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableStatistic;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.generator.SeriesSqlConnector.OPTION_START;
import static com.hazelcast.jet.sql.impl.connector.generator.SeriesSqlConnector.OPTION_STEP;
import static com.hazelcast.jet.sql.impl.connector.generator.SeriesSqlConnector.OPTION_STOP;
import static java.util.Arrays.asList;

public final class SeriesGeneratorTableFunction extends JetTableFunction {

    public static final SeriesGeneratorTableFunction GENERATE_SERIES = new SeriesGeneratorTableFunction();

    private static final String SCHEMA_NAME_SERIES = "series";

    private static final List<FunctionParameter> PARAMETERS = asList(
            new JetTableFunctionParameter(0, OPTION_START, SqlTypeName.INTEGER, true),
            new JetTableFunctionParameter(1, OPTION_STOP, SqlTypeName.INTEGER, true),
            new JetTableFunctionParameter(2, OPTION_STEP, SqlTypeName.INTEGER, false)
    );

    private SeriesGeneratorTableFunction() {
        super(SeriesSqlConnector.INSTANCE);
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return PARAMETERS;
    }

    @Override
    protected HazelcastTable toTable(List<Object> arguments) {
        int start = (Integer) arguments.get(0);
        int stop = (Integer) arguments.get(1);
        int step = arguments.get(2) != null ? (Integer) arguments.get(2) : 1;
        SeriesTable table = SeriesSqlConnector.createTable(
                SCHEMA_NAME_SERIES,
                randomName(),
                start,
                stop,
                step
        );
        return new HazelcastTable(table, new HazelcastTableStatistic(table.numberOfItems()));
    }

    private static String randomName() {
        return "series_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
