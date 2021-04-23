/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
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
