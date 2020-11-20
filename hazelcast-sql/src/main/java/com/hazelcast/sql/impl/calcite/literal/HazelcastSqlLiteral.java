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

package com.hazelcast.sql.impl.calcite.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeBridge;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

// TODO: Convert to a static method(s)
public abstract class HazelcastSqlLiteral extends SqlNodeBridge {

    protected final SqlLiteral original;
    protected final Object value;
    protected final SqlTypeName typeName;

    public HazelcastSqlLiteral(SqlLiteral original, Object value, SqlTypeName typeName) {
        super(original.getParserPosition());

        this.original = original;
        this.value = value;
        this.typeName = typeName;
    }

    public static HazelcastSqlLiteral convert(SqlLiteral original) {
        // Do no convert symbols.
        if (original.getTypeName() == SqlTypeName.SYMBOL) {
            return null;
        }

        if (original instanceof SqlNumericLiteral) {
            return HazelcastSqlNumericLiteral.create((SqlNumericLiteral) original);
        }

        if (original instanceof SqlCharStringLiteral) {
            return new HazelcastSqlTypedLiteral(original, original.getValueAs(String.class), SqlTypeName.VARCHAR);
        }

        return new HazelcastSqlTypedLiteral(original, original.getValue(), original.getTypeName());
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

    @Override
    public final void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        original.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        throw new UnsupportedOperationException("Should not be called!");
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!this.equals(node)) {
            return litmus.fail("{} != {}", this, node);
        }

        return litmus.succeed();
    }
}
