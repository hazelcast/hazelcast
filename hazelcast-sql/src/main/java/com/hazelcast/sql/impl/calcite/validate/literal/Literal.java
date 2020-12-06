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

package com.hazelcast.sql.impl.calcite.validate.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class Literal {

    protected final SqlLiteral original;
    protected final Object value;
    protected final SqlTypeName typeName;

    public Literal(SqlLiteral original, Object value, SqlTypeName typeName) {
        this.original = original;
        this.value = value;
        this.typeName = typeName;
    }

    public static Literal convert(SqlLiteral original) {
        // Do no convert symbols.
        if (original.getTypeName() == SqlTypeName.SYMBOL) {
            return null;
        }

        if (original instanceof SqlNumericLiteral) {
            return NumericLiteral.create((SqlNumericLiteral) original);
        }

        if (original instanceof SqlCharStringLiteral) {
            return new TypedLiteral(original, original.getValueAs(String.class), SqlTypeName.VARCHAR);
        }

        return new TypedLiteral(original, original.getValue(), original.getTypeName());
    }

    public Object getValue() {
        return value;
    }

    public SqlLiteral getOriginal() {
        return original;
    }

    public SqlTypeName getTypeName() {
        return typeName;
    }

    public abstract RelDataType getType(HazelcastTypeFactory typeFactory);
}
