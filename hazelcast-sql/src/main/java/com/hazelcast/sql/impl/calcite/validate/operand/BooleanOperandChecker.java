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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.param.StrictParameterConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.type.SqlTypeName;

public final class BooleanOperandChecker extends AbstractTypedOperandChecker {

    public static final BooleanOperandChecker INSTANCE = new BooleanOperandChecker();

    private BooleanOperandChecker() {
        super(SqlTypeName.BOOLEAN);
    }

    @Override
    protected ParameterConverter parameterConverter(SqlDynamicParam operand) {
        return new StrictParameterConverter(operand.getIndex(), operand.getParserPosition(), QueryDataType.BOOLEAN);
    }
}
