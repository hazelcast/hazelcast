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

import static com.hazelcast.jet.sql.impl.connector.generator.StreamSqlConnector.OPTION_RATE;
import static java.util.Collections.singletonList;

public final class StreamGeneratorTableFunction extends JetTableFunction {

    public static final StreamGeneratorTableFunction GENERATE_STREAM = new StreamGeneratorTableFunction();

    private static final String SCHEMA_NAME_STREAM = "stream";

    private static final List<FunctionParameter> PARAMETERS = singletonList(
            new JetTableFunctionParameter(0, OPTION_RATE, SqlTypeName.INTEGER, true)
    );

    private StreamGeneratorTableFunction() {
        super(StreamSqlConnector.INSTANCE);
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return PARAMETERS;
    }

    @Override
    protected HazelcastTable toTable(List<Object> arguments) {
        int rate = (Integer) arguments.get(0);
        StreamTable table = StreamSqlConnector.createTable(SCHEMA_NAME_STREAM, randomName(), rate);

        return new HazelcastTable(table, new HazelcastTableStatistic(Integer.MAX_VALUE));
    }

    private static String randomName() {
        return "stream_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
