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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.calcite.validate.operand.OperandChecker;
import org.apache.calcite.sql.type.SqlTypeName;

public class JetTableFunctionParameter {

    private final int ordinal;
    private final String name;
    private final SqlTypeName type;
    private final OperandChecker checker;

    public JetTableFunctionParameter(int ordinal, String name, SqlTypeName type, OperandChecker checker) {
        this.ordinal = ordinal;
        this.name = name;
        this.type = type;
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

    public OperandChecker checker() {
        return checker;
    }
}
