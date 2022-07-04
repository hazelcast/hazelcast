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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.validate.operand.OperandChecker;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastTableFunctionParameter {

    private final int ordinal;
    private final String name;
    private final SqlTypeName type;
    private final boolean optional;
    private final OperandChecker checker;

    public HazelcastTableFunctionParameter(
            int ordinal,
            String name,
            SqlTypeName type,
            boolean optional,
            OperandChecker checker
    ) {
        this.ordinal = ordinal;
        this.name = name;
        this.type = type;
        this.optional = optional;
        this.checker = checker;
    }

    public int ordinal() {
        return ordinal;
    }

    public String name() {
        return name;
    }

    public SqlTypeName type() {
        return type;
    }

    public boolean optional() {
        return optional;
    }

    public OperandChecker checker() {
        return checker;
    }
}
