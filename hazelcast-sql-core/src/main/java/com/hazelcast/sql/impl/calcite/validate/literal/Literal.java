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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A convenient class to resolve literal types. See {@link LiteralUtils#literal(SqlNode)}.
 */
public abstract class Literal {

    protected final Object value;
    protected final SqlTypeName typeName;

    public Literal(Object value, SqlTypeName typeName) {
        this.value = value;
        this.typeName = typeName;
    }

    public Object getValue() {
        return value;
    }

    public String getStringValue() {
        return value != null ? value.toString() : null;
    }

    public SqlTypeName getTypeName() {
        return typeName;
    }

    public abstract RelDataType getType(HazelcastTypeFactory typeFactory);
}
