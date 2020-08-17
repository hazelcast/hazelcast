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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

public class TrimFunction extends BiExpression<String> implements IdentifiedDataSerializable {

    private static final CharacterTester SPACE_ONLY = new SingleCharacterTester(' ');

    private boolean leading;
    private boolean trailing;

    public TrimFunction() {
        // No-op.
    }

    private TrimFunction(Expression<?> operand1, Expression<?> operand2, boolean leading, boolean trailing) {
        super(operand1, operand2);

        this.leading = leading;
        this.trailing = trailing;
    }

    public static TrimFunction create(Expression<?> operand1, Expression<?> operand2, boolean leading, boolean trailing) {
        return new TrimFunction(operand1, operand2, leading, trailing);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String input = StringExpressionUtils.asVarchar(operand1, row, context);

        if (input == null) {
            return null;
        }

        String characters = operand2 != null ? StringExpressionUtils.asVarchar(operand2, row, context) : null;

        return trim(input, characters);
    }

    private String trim(String input, String characters) {
        if (input.length() == 0) {
            return "";
        }

        CharacterTester tester = characters == null ? SPACE_ONLY : characters.length() == 1
            ? new SingleCharacterTester(characters.charAt(0)) : new MultipleCharacterTester(characters.toCharArray());

        int from = 0;
        int to = input.length();

        if (leading) {
            while (from < input.length() && tester.test(input.charAt(from))) {
                from++;
            }
        }

        if (trailing) {
            while (to > from && tester.test(input.charAt(to - 1))) {
                to--;
            }
        }

        if (from == 0 && to == input.length()) {
            return input;
        } else {
            return input.substring(from, to);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_TRIM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(leading);
        out.writeBoolean(trailing);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        leading = in.readBoolean();
        trailing = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        TrimFunction that = (TrimFunction) o;

        return leading == that.leading && trailing == that.trailing;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (leading ? 1 : 0);
        result = 31 * result + (trailing ? 1 : 0);
        return result;
    }

    private interface CharacterTester {
        boolean test(char c);
    }

    private static class SingleCharacterTester implements CharacterTester {

        private final char expected;

        private SingleCharacterTester(char expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(char c) {
            return c == expected;
        }
    }

    private static class MultipleCharacterTester implements CharacterTester {

        private final char[] expected;

        private MultipleCharacterTester(char[] expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(char c) {
            for (char value : expected) {
                if (c == value) {
                    return true;
                }
            }

            return false;
        }
    }
}

