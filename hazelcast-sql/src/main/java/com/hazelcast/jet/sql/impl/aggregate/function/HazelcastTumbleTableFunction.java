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

package com.hazelcast.jet.sql.impl.aggregate.function;

import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.operand.DescriptorOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.RowOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.SqlDaySecondIntervalOperandChecker;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static java.util.Arrays.asList;

public class HazelcastTumbleTableFunction extends HazelcastWindowTableFunction {

    private static final List<HazelcastTableFunctionParameter> PARAMETERS = asList(
            new HazelcastTableFunctionParameter(0, "input", SqlTypeName.ROW, false, RowOperandChecker.INSTANCE),
            new HazelcastTableFunctionParameter(
                    1,
                    "time_column",
                    SqlTypeName.COLUMN_LIST,
                    false,
                    DescriptorOperandChecker.INSTANCE
            ),
            new HazelcastTableFunctionParameter(
                    2,
                    "window_size",
                    SqlTypeName.INTERVAL_SECOND,
                    false,
                    SqlDaySecondIntervalOperandChecker.INSTANCE
            )
    );

    public HazelcastTumbleTableFunction() {
        super(SqlKind.TUMBLE, new WindowOperandMetadata(PARAMETERS));
    }
}
