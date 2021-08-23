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

package com.hazelcast.jet.sql.impl.validate.literal;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

public class IntervalLiteral extends Literal {

    private final SqlIntervalQualifier intervalQualifier;

    public IntervalLiteral(SqlIntervalLiteral.IntervalValue value, SqlTypeName typeName) {
        super(value, typeName);

        intervalQualifier = value.getIntervalQualifier();
    }

    @Override
    public RelDataType getType(HazelcastTypeFactory typeFactory) {
        return typeFactory.createSqlIntervalType(intervalQualifier);
    }
}
